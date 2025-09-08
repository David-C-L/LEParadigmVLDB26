#include "BOSSParquetReaderEngine.hpp"
#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <mutex>
#include <cmath>
#include <utility>
#include <unordered_map>
#include <vector>
#include <memory>
#include <algorithm>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include <parquet/types.h>
#include <parquet/statistics.h>
#include <arrow/visitor.h>
#include <arrow/visit_array_inline.h>
#include <arrow/util/decimal.h>
#include <parquet/schema.h>
#include <boost/dynamic_bitset.hpp>

#include <typeinfo>
// #define DEBUG

#ifdef DEBUG
#ifndef _MSC_VER
#include <cxxabi.h>  // Required for abi::__cxa_demangle on GCC/Clang
#endif
#endif

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;
using boss::ExpressionArguments;
using boss::expressions::ExpressionSpanArgument;
using boss::expressions::ExpressionSpanArguments;

using boss::Expression;

constexpr static int64_t FETCH_PADDING = 0;
constexpr static int64_t FETCH_ALIGNMENT = 4096;
constexpr static int64_t MAX_RANGES = 1;

namespace boss::engines::ParquetReader {
namespace utilities {
template <typename Func> class ArrowArrayVisitor : public arrow::ArrayVisitor {
public:
  explicit ArrowArrayVisitor(Func&& func) : func(std::forward<Func>(func)) {}

  arrow::Status Visit(arrow::NullArray const& /*arrowArray*/) override {
    return arrow::Status::ExecutionError("unsupported arrow type");
  }

  template <typename ArrayType> arrow::Status Visit(ArrayType const& arrowArray) {
    func(arrowArray);
    return arrow::Status::OK();
  }

private:
  Func func;
};
} // namespace utilities

#ifdef DEBUG
  template <typename T> std::string get_type_name() {
    const char *typeName = typeid(T).name();

#ifndef _MSC_VER
    // Demangle the type name on GCC/Clang
    int status = -1;
    std::unique_ptr<char, void (*)(void *)> res{
      abi::__cxa_demangle(typeName, nullptr, nullptr, &status), std::free};
    return (status == 0 ? res.get() : typeName);
#else
    // On MSVC, typeid().name() returns a human-readable name.
    return typeName;
#endif
  }
#endif

  void printParquetSchema(std::unique_ptr<parquet::arrow::FileReader>& parquetReader) {
    std::shared_ptr<arrow::Schema> schema;
    auto status = parquetReader->GetSchema(&schema);
    if (!status.ok()) {
        throw std::runtime_error("Error getting schema: " + status.ToString());
    }

    std::cout << schema->ToString() << std::endl;
  }
  
boss::Expression applyEngine(Expression &&e, EvalFunction eval) {
  auto *r = new BOSSExpression{std::move(e)};
  auto *oldWrapper = r;
  r = eval(r);
  delete oldWrapper;
  auto result = std::move(r->delegate);
  delete r;
  return std::move(result);
}

  int64_t getFileLength(const std::string &url, EvalFunction loader) {
    ExpressionArguments args;
    args.push_back(url);
    auto getFileLenExpr = boss::ComplexExpression("GetFileLength"_, {}, std::move(args), {});

    return get<int64_t>(applyEngine(std::move(getFileLenExpr), loader));
  }


boss::Expression createFetchExpression(const std::string &url,
                              std::vector<int64_t> &bounds) {
  ExpressionArguments args;
  args.push_back(std::move("List"_(boss::Span<int64_t>(bounds))));
  args.push_back(url);
  args.push_back((int64_t)FETCH_PADDING);
  args.push_back((int64_t)FETCH_ALIGNMENT);
  args.push_back((int64_t)MAX_RANGES);

  return boss::ComplexExpression("Fetch"_, {}, std::move(args), {});
}

  
boss::Span<int8_t> getByteSequence(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "ByteSequence"_) {
    throw std::runtime_error(
        "Unsupported expression: deserialiseByteSequence function does not "
        "support deserialising from non-ByteSequence_ expressions");
  }

  if (spans.empty()) {
    throw std::runtime_error(
        "Parquet Reader: No spans present in ByteSequence_ expression");
  }

  auto &typedSpan = spans[0];
  if (!std::holds_alternative<boss::Span<int8_t>>(typedSpan)) {
    throw std::runtime_error(
        "unsupported span type: ByteSequence span is not an int8_t span");
  }
  auto &byteSpan = std::get<boss::Span<int8_t>>(typedSpan);
  return std::move(byteSpan);
}

boss::Span<int8_t> loadBoundsIntoSpan(const std::string &url,
                           std::vector<int64_t> &bounds,
                           EvalFunction &loader) {
  auto fetchExpr = createFetchExpression(url, bounds);
  auto byteSeqExpr = std::get<boss::ComplexExpression>(
      applyEngine(std::move(fetchExpr), loader));
  return std::move(getByteSequence(std::move(byteSeqExpr)));
}

  std::vector<int64_t> getFooterBounds(boss::Span<int8_t> &byteSpan, int64_t fileLength) {
    int32_t footerLength = 0;
    footerLength |= (static_cast<uint8_t>(byteSpan[fileLength - 8]))
      | (static_cast<uint8_t>(byteSpan[fileLength - 7]) << 8)
      | (static_cast<uint8_t>(byteSpan[fileLength - 6]) << 16)
      | (static_cast<uint8_t>(byteSpan[fileLength - 5]) << 24);
    #ifdef DEBUG
    std::cout << "FOOTER LEN: " << footerLength << " STARTS AT: " << fileLength - footerLength - 8 << std::endl;
    #endif
    return {fileLength - footerLength - 8, fileLength};
  }
  
  std::vector<int64_t> getColumnWithMinMaxBounds(std::unique_ptr<parquet::arrow::FileReader> &parquetReader, boost::dynamic_bitset<uint8_t> &colChunkIDs) {
    std::vector<int64_t> resultBounds;
    
    std::shared_ptr<parquet::FileMetaData> parquetMetadata = parquetReader->parquet_reader()->metadata();

    for (auto rowGroupI = 0, currColChunkI = 0; rowGroupI < parquetMetadata->num_row_groups(); rowGroupI++) {
      std::shared_ptr<parquet::RowGroupMetaData> rowGroupMetadata = parquetMetadata->RowGroup(rowGroupI);
      for (auto colChunkI = 0; colChunkI < rowGroupMetadata->num_columns(); colChunkI++, currColChunkI++) {
	std::string colName = parquetMetadata->schema()->Column(colChunkI)->name();
        if (colChunkIDs.test(currColChunkI)) {
	  std::shared_ptr<parquet::ColumnChunkMetaData> colChunkMetadata = rowGroupMetadata->ColumnChunk(colChunkI);
	  int64_t colStartOffset = colChunkMetadata->data_page_offset();
	  int64_t colEndOffset = colStartOffset + colChunkMetadata->total_compressed_size();
	  resultBounds.push_back(colStartOffset);
	  resultBounds.push_back(colEndOffset);
	  
#ifdef DEBUG
	  std::cout << "Row Group: " << rowGroupI << " Column Chunk: " << colChunkI << " Overall Chunk: " << currColChunkI << " loading at offsets:" << std::endl;
	  std::cout << "  - Start offset: " << colStartOffset << std::endl;
	  std::cout << "  - End offset: " << colEndOffset << std::endl;
#endif
	}
      }
    }
    return std::move(resultBounds);
  }
  
  std::vector<int64_t> getColumnWithMinMaxBounds(std::unique_ptr<parquet::arrow::FileReader> &parquetReader, boost::dynamic_bitset<uint8_t> &colChunkIDs, boss::Span<int8_t> &byteSpan, EvalFunction &loader, const std::string &url) {
    std::vector<int64_t> resultBounds;
    
    std::shared_ptr<parquet::FileMetaData> parquetMetadata = parquetReader->parquet_reader()->metadata();
    std::unordered_set<int32_t> colIs;
    std::unordered_set<int32_t> rowGroupIs;
    for (auto rowGroupI = 0, currColChunkI = 0; rowGroupI < parquetMetadata->num_row_groups(); rowGroupI++) {
      std::shared_ptr<parquet::RowGroupMetaData> rowGroupMetadata = parquetMetadata->RowGroup(rowGroupI);
      for (auto colChunkI = 0; colChunkI < rowGroupMetadata->num_columns(); colChunkI++, currColChunkI++) {
	
	std::string colName = parquetMetadata->schema()->Column(colChunkI)->name();
        if (colChunkIDs.test(currColChunkI)) {
	  colIs.insert(colChunkI);
	  rowGroupIs.insert(rowGroupI);
	  std::shared_ptr<parquet::ColumnChunkMetaData> colChunkMetadata = rowGroupMetadata->ColumnChunk(colChunkI);
	  int64_t maxSize = colChunkMetadata->total_compressed_size();
	  if (colChunkMetadata->dictionary_page_offset() != 0) {
	    resultBounds.push_back(colChunkMetadata->dictionary_page_offset());
	    resultBounds.push_back(colChunkMetadata->dictionary_page_offset() + maxSize);
	  }
	  int64_t colStartOffset = colChunkMetadata->data_page_offset();
	  int64_t colEndOffset = colStartOffset + maxSize;
	  resultBounds.push_back(colStartOffset);
	  resultBounds.push_back(colEndOffset);
	}
      }
      byteSpan = loadBoundsIntoSpan(url, resultBounds, loader);
      resultBounds.clear();
    }
    return std::move(resultBounds);
  }

  arrow::Decimal128 fixedLenByteArrayToDecimal128(parquet::FixedLenByteArray& byteArray) {
    int32_t len = 16;
    const uint8_t* bytes = byteArray.ptr;
    arrow::Decimal128 res = arrow::Decimal128::FromBigEndian(bytes, len).ValueOrDie();
    return res;
  }

  template <typename T>
  bool isChunkInRange(const parquet::Statistics* colChunkStats, std::vector<std::pair<double_t, double_t>> &minMaxValues) {
    const auto* typedStats = dynamic_cast<const parquet::TypedStatistics<T>*>(colChunkStats);
    auto minValue = typedStats->min();
    auto maxValue = typedStats->max();
    
#ifdef DEBUG
    std::cout << "  - Min Value: " << minValue << std::endl;
    std::cout << "  - Max Value: " << maxValue << std::endl;
#endif
    bool isChunkInRange = false;
    for (const auto& [lb, ub] : minMaxValues) {
      isChunkInRange |= ((lb >= minValue && lb <= maxValue) || (ub >= minValue && ub <= maxValue));
    }
    return isChunkInRange;
  }

  template <>
  bool isChunkInRange<parquet::FLBAType>(const parquet::Statistics* colChunkStats, std::vector<std::pair<double_t, double_t>> &minMaxValues) {
    auto scale = 23;
    const auto* typedStats = dynamic_cast<const parquet::TypedStatistics<parquet::FLBAType>*>(colChunkStats);
    auto fblaMinValue = typedStats->min();
    auto fblaMaxValue = typedStats->max();
     
    auto minValue = fixedLenByteArrayToDecimal128(fblaMinValue).ToDouble(scale);
    auto maxValue = fixedLenByteArrayToDecimal128(fblaMaxValue).ToDouble(scale);
    
#ifdef DEBUG
    std::cout << "  - Min Value: " << minValue << std::endl;
    std::cout << "  - Max Value: " << maxValue << std::endl;
#endif
    
    bool isChunkInRange = false;
    for (const auto& [lb, ub] : minMaxValues) {
      isChunkInRange |= ((lb >= minValue && lb <= maxValue) || (ub >= minValue && ub <= maxValue));
    }
    return isChunkInRange;
  }

  bool isChunkInRange(std::shared_ptr<parquet::ColumnChunkMetaData> &colChunkMetadata, const parquet::Statistics* colChunkStats, std::vector<std::pair<double_t, double_t>> &minMaxValues) {
    switch(colChunkMetadata->type()) {
    case parquet::Type::INT32:
      return isChunkInRange<parquet::Int32Type>(colChunkStats, minMaxValues);
    case parquet::Type::INT64:
      return isChunkInRange<parquet::Int64Type>(colChunkStats, minMaxValues);
    case parquet::Type::FLOAT:
      return isChunkInRange<parquet::FloatType>(colChunkStats, minMaxValues);
    case parquet::Type::DOUBLE:
      return isChunkInRange<parquet::DoubleType>(colChunkStats, minMaxValues);
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return isChunkInRange<parquet::FLBAType>(colChunkStats, minMaxValues);
    default:
      throw std::runtime_error("Unsupported column type for min/max statistics with requested type: " + parquet::TypeToString(colChunkMetadata->type()));
    }
  }

  boost::dynamic_bitset<uint8_t> getColChunkIDsToLoad(std::unique_ptr<parquet::arrow::FileReader> &parquetReader, std::unordered_map<std::string, std::vector<std::pair<double_t, double_t>>> &columnMinMaxMap) {
    
    std::shared_ptr<parquet::FileMetaData> parquetMetadata = parquetReader->parquet_reader()->metadata();
    auto numRowGroups = parquetMetadata->num_row_groups();
    auto numColumns = parquetMetadata->num_columns();

#ifdef DEBUG
    std::cout << "Row Group Count: " << numRowGroups << " Column Count: " << numColumns << std::endl;
#endif
    
    boost::dynamic_bitset<uint8_t> rowGroupIDs(numRowGroups, 0);
    boost::dynamic_bitset<uint8_t> columnIDs(numColumns, 0);

    for (auto colI = 0; colI < numColumns; colI++) {
      std::string colName = parquetMetadata->schema()->Column(colI)->name();
      columnIDs[colI] = (columnMinMaxMap.find(colName) != columnMinMaxMap.end());
    }

    bool anyHasMinMax = false;
    for (auto rowGroupI = 0; rowGroupI < numRowGroups; rowGroupI++) {
      std::shared_ptr<parquet::RowGroupMetaData> rowGroupMetadata = parquetMetadata->RowGroup(rowGroupI);
      
      for (auto colChunkI = 0; colChunkI < rowGroupMetadata->num_columns(); colChunkI++) {
	
	std::string colName = parquetMetadata->schema()->Column(colChunkI)->name();
	if (columnMinMaxMap.find(colName) != columnMinMaxMap.end()) {
	  std::shared_ptr<parquet::ColumnChunkMetaData> colChunkMetadata = rowGroupMetadata->ColumnChunk(colChunkI);
	  auto &minMaxValues = columnMinMaxMap[colName];
	  anyHasMinMax = colChunkMetadata->is_stats_set() && !minMaxValues.empty();
	}
      }
    }

    for (auto rowGroupI = 0; rowGroupI < numRowGroups; rowGroupI++) {
      std::shared_ptr<parquet::RowGroupMetaData> rowGroupMetadata = parquetMetadata->RowGroup(rowGroupI);
      bool getRowGroup = false;
#ifdef DEBUG
      std::cout << "Row Group " << rowGroupI << " has " << rowGroupMetadata->num_columns()  << " columns: " << std::endl;
#endif
      for (auto colChunkI = 0; colChunkI < rowGroupMetadata->num_columns(); colChunkI++) {
	
	std::string colName = parquetMetadata->schema()->Column(colChunkI)->name();
#ifdef DEBUG
	std::cout << "Column " << colChunkI << " is " << colName << std::endl;
#endif
	if (columnMinMaxMap.find(colName) != columnMinMaxMap.end()) {
	  std::shared_ptr<parquet::ColumnChunkMetaData> colChunkMetadata = rowGroupMetadata->ColumnChunk(colChunkI);
	  auto &minMaxValues = columnMinMaxMap[colName];
#ifdef DEBUG
	  std::cout << " (RETRIEVED)" << std::endl;
	  std::cout << " Type: " << parquet::TypeToString(colChunkMetadata->type()) << std::endl;
	  std::cout << " Num Min/Max To Check: " << minMaxValues.size() << std::endl;
#endif
	  if (!anyHasMinMax) {
	    getRowGroup = true;
	  } else {
	    if (colChunkMetadata->is_stats_set() && !minMaxValues.empty()) {

	      const parquet::Statistics* colChunkStats = colChunkMetadata->statistics().get();
	      if (colChunkStats != nullptr && colChunkStats->HasMinMax()) {
		bool isChunkWithinRange = isChunkInRange(colChunkMetadata, colChunkStats, minMaxValues);
		getRowGroup |= isChunkWithinRange;
#ifdef DEBUG
		std::cout << "  GOT MIN/MAX" << " and isChunkWithinRange (" << isChunkWithinRange << ") and getRowGroup (" << getRowGroup << ")" << std::endl;
#endif
	      } else {
		throw std::runtime_error("Column: " + colName + " Chunk: " + std::to_string(colChunkI) + " has requested min-max values but no accompanying statistics in file.");
	      }
	    }
	  }
	}
      }
      rowGroupIDs[rowGroupI] = getRowGroup;
    }

    boost::dynamic_bitset<uint8_t> allColChunkIDs(numRowGroups * numColumns, 0);
    for (auto i = 0, currColChunkI = 0; i < rowGroupIDs.size(); i++) {
      if (rowGroupIDs.test(i)) {
	for (auto j = 0; j < columnIDs.size(); j++, currColChunkI++) {
	  allColChunkIDs[currColChunkI] = columnIDs[j];
	}
      } else {
	for (auto j = 0; j < columnIDs.size(); j++, currColChunkI++) {
	  allColChunkIDs[currColChunkI] = 0;
	}
      }
    }

    return std::move(allColChunkIDs);
  }
  
  void readColumnChunkIntoSpans(std::unique_ptr<parquet::arrow::FileReader> &parquetReader, int64_t rowGroupI, int64_t colChunkI, ExpressionSpanArguments &resSpans) {
    std::shared_ptr<parquet::arrow::RowGroupReader> rowGroupReader = parquetReader->RowGroup(rowGroupI);
    std::shared_ptr<parquet::arrow::ColumnChunkReader> colChunkReader = rowGroupReader->Column(colChunkI);
    std::shared_ptr<arrow::ChunkedArray> colChunkArray;

    arrow::Status status = colChunkReader->Read(&colChunkArray);
    if (!status.ok()) {
        throw std::runtime_error("Error reading column chunk: " + status.ToString());
    }
    
    for (auto chunkI = 0; chunkI < colChunkArray->num_chunks(); chunkI++) {
      auto arrowArrayPtr = colChunkArray->chunk(chunkI);
      auto visitor = utilities::ArrowArrayVisitor([&arrowArrayPtr,
                                                       &resSpans](auto const& columnArray) {
	if constexpr(std::is_convertible_v<decltype(columnArray), arrow::StringArray const&>) {
	  std::vector<std::string> stringVec;
	  stringVec.reserve(columnArray.length());
	  for (auto i = 0; i < columnArray.length(); i++) {
	    if (columnArray.IsNull(i)) {
	      stringVec.push_back("");
	    } else {
	      stringVec.push_back(columnArray.GetString(i));
	    }
	  }
	  auto stringSpan = boss::Span<std::string>(std::move(std::vector(std::move(stringVec))));
	  resSpans.emplace_back(std::move(stringSpan));
	  return;
	} else if constexpr(std::is_convertible_v<decltype(columnArray),
			    arrow::PrimitiveArray const&>) {
	  using ElementType = std::decay_t<decltype(columnArray.Value(0))> const;
	  if constexpr(std::is_constructible_v<expressions::ExpressionSpanArgument,
		       boss::Span<ElementType>> &&
		       std::is_constructible_v<boss::Span<ElementType>, ElementType*, int,
		       std::function<void(void)>>) {
	    resSpans.emplace_back(boss::Span<ElementType>(
							  columnArray.raw_values(), columnArray.length(), [stored = arrowArrayPtr]() {}));
	    return;
	  }
	}
#ifdef DEBUG
	throw std::runtime_error("unsupported arrow array type " + get_type_name<std::decay_t<decltype(columnArray)>>());
#endif
	throw std::runtime_error("unsupported arrow array type");
      });

      status = arrow::VisitArrayInline(*arrowArrayPtr, &visitor);
      if (!status.ok()) {
        throw std::runtime_error("Failed to visit arrow array: " + status.ToString());
      } 
    }
  }

  std::unordered_map<std::string, ExpressionSpanArguments> collectSpans(std::unique_ptr<parquet::arrow::FileReader> &parquetReader, std::vector<std::string> &colNames, boost::dynamic_bitset<uint8_t> &colChunkIDs) {

    std::unordered_map<std::string, ExpressionSpanArguments> colToSpansMap;
    for (auto& colName : colNames) {
      ExpressionSpanArguments spanArgs;
      colToSpansMap[colName] = std::move(spanArgs);
    }
    
    std::shared_ptr<parquet::FileMetaData> parquetMetadata = parquetReader->parquet_reader()->metadata();
    auto numRowGroups = parquetMetadata->num_row_groups();

    for (auto rowGroupI = 0, currColChunkI = 0; rowGroupI < numRowGroups; rowGroupI++) {
      std::shared_ptr<parquet::RowGroupMetaData> rowGroupMetadata = parquetMetadata->RowGroup(rowGroupI);

#ifdef DEBUG
      std::cout << "Row Group " << rowGroupI << " has " << rowGroupMetadata->num_columns()  << " columns: " << std::endl;
#endif
      for (auto colChunkI = 0; colChunkI < rowGroupMetadata->num_columns(); colChunkI++, currColChunkI++) {
	std::string colName = parquetMetadata->schema()->Column(colChunkI)->name();
#ifdef DEBUG
	std::cout << "Column " << colChunkI << " is " << colName << " (Overall Column Chunk: " << currColChunkI << " is retrieved: " << colChunkIDs.test(currColChunkI) << ")";
#endif
	if (colChunkIDs.test(currColChunkI)) {
#ifdef DEBUG
	  std::cout << " and being read into spans";
#endif
	  auto &colSpanArgs = colToSpansMap[colName];
	  readColumnChunkIntoSpans(parquetReader, rowGroupI, colChunkI, colSpanArgs);
	}
#ifdef DEBUG
	std::cout << std::endl;
#endif
      }
    }
    return std::move(colToSpansMap);
  }

  boss::Expression getColumnsFromBitsetInTableExpression(std::unique_ptr<parquet::arrow::FileReader> &parquetReader, std::vector<std::string> &colNames, boost::dynamic_bitset<uint8_t> &colChunkIDs) {
    ExpressionArguments tableArgs;
    auto colToSpansMap = collectSpans(parquetReader, colNames, colChunkIDs);

    bool createdIndices = false;
    
    for (auto& [colName, colSpans] : colToSpansMap) {
      ExpressionArguments colArgs;
      auto listExpr = boss::ComplexExpression("List"_, {}, {}, std::move(colSpans));
      auto colHead = boss::Symbol(colName);
      colArgs.emplace_back(std::move(listExpr));
      auto colExpr = boss::ComplexExpression(std::move(colHead), {}, std::move(colArgs), {});
      tableArgs.emplace_back(std::move(colExpr));
    }
    auto tableExpr = boss::ComplexExpression("Table"_, {}, std::move(tableArgs), {});
    return std::move(tableExpr);
  }

  std::vector<std::pair<double_t, double_t>> getMinMaxList(boss::ComplexExpression &&expression) {
    std::vector<std::pair<double_t, double_t>> minMaxList;
    auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
    if (head != "List"_ || dynamics.size() % 2 != 0) {
      return minMaxList;
    }

    for (int64_t i = 0; i < dynamics.size(); i += 2) {
      minMaxList.emplace_back(get<double_t>(std::move(dynamics[i])), get<double_t>(std::move(dynamics[i+1])));
    }

    return minMaxList;
  }

  std::unordered_map<std::string, std::vector<std::pair<double_t, double_t>>> getColumnsWithMinMax(boss::ComplexExpression &&expression) {
    std::unordered_map<std::string, std::vector<std::pair<double_t, double_t>>> colMinMaxMap;
    auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
    if (head != "List"_) {
      return colMinMaxMap;
    }

    std::for_each(std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()),
                [&colMinMaxMap](auto &&arg) {
                  if (std::holds_alternative<boss::ComplexExpression>(arg)) {
		    auto [colHead, unused2_, colDynamics, colSpans] = std::move(get<boss::ComplexExpression>(std::move(arg))).decompose();
#ifdef DEBUG
                    std::cout << "SELECTED: " << colHead << std::endl;
#endif
		    auto minMaxList = getMinMaxList(get<boss::ComplexExpression>(std::move(colDynamics[0])));
		    colMinMaxMap[colHead.getName()] = std::move(minMaxList);
                  }
                });

    return std::move(colMinMaxMap);
  }


boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression)
              -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

            if (head == "GetColumnsFromParquet"_) {
	      const auto &loaderPath = get<std::string>(std::move(dynamics[0]));
	      EvalFunction loader = reinterpret_cast<EvalFunction>(libraries.at(loaderPath).evaluateFunction);
	      const auto &url = get<std::string>(std::move(dynamics[1]));
	      auto columns = getColumnsWithMinMax(std::move(get<boss::ComplexExpression>(std::move(dynamics[2]))));
	      std::vector<std::string> colNames;
	      for (auto& [colName, val] : columns) {
		colNames.push_back(colName);
	      }
	      
	      int64_t fileLength = getFileLength(url, loader);
	      boss::Span<int8_t> byteSpan;
	      std::vector<int64_t> metadataBounds1 = {fileLength-8, fileLength-1};
	      std::vector<int64_t> metadataBounds2 = {0, 4};
	      byteSpan = loadBoundsIntoSpan(url, metadataBounds1, loader);
	      byteSpan = loadBoundsIntoSpan(url, metadataBounds2, loader);

	      std::vector<int64_t> footerBounds = getFooterBounds(byteSpan, fileLength);
	      byteSpan = loadBoundsIntoSpan(url, footerBounds, loader);
	      
	      auto mutableArrowBuffer = std::make_shared<arrow::MutableBuffer>((uint8_t*) byteSpan.begin(), byteSpan.size());
	      
	      auto arrowBufferReader = std::make_shared<arrow::io::BufferReader>(mutableArrowBuffer);

	      std::unique_ptr<parquet::arrow::FileReader> parquetReader;
	      auto status = parquet::arrow::OpenFile(arrowBufferReader, arrow::default_memory_pool(), &parquetReader);
	      if (!status.ok()) {
	        throw std::runtime_error("Error opening parquet file: " + status.ToString());
	      }

	      auto colChunksToLoadBitset = getColChunkIDsToLoad(parquetReader, columns);

	      if (currColChunksMap.find(url) != currColChunksMap.end()) {
		auto &currColChunksToLoadBitset = currColChunksMap[url];
		currColChunksToLoadBitset &= colChunksToLoadBitset;
	      } else {
		currColChunksMap[url] = std::move(colChunksToLoadBitset);
	      }

	      auto &currColChunksToLoadBitset = currColChunksMap[url];
	      
	      auto columnBounds = getColumnWithMinMaxBounds(parquetReader, currColChunksToLoadBitset, byteSpan, loader, url);
	      auto tableExpr = getColumnsFromBitsetInTableExpression(parquetReader, colNames, currColChunksToLoadBitset);
	      
	      return std::move(tableExpr);
            } else if (head == "ClearParquetReaderCaches"_) {
	      currColChunksMap.clear();
	      return "CachesCleared"_;
	    } else if (head == "GetEngineCapabilities"_) {
	      return "List"_("GetColumnsFromParquet"_, "ClearParquetReaderCaches"_);
	    }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return boss::ComplexExpression(
                std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

} // namespace boss::engines::ParquetReader

static auto &enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<boss::engines::ParquetReader::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::ParquetReader::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
