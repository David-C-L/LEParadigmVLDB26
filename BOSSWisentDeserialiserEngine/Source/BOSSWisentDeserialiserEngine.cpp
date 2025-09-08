#include "BOSSWisentDeserialiserEngine.hpp"
#include "InstantiationMacros.hpp"
#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <type_traits>
#include <unordered_set>
#include <robin_hood.h>
#include <queue>
#include <future>
#include <thread>
#include <immintrin.h>
#include <cstdint>
#include <concepts>
#include <cmath>

// #define DEBUG

#ifdef DEBUG
#ifndef _MSC_VER
#include <cxxabi.h>
#include <memory>
#endif
#endif

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::ExpressionArguments;
using boss::Span;
using boss::Symbol;
using boss::expressions::ExpressionSpanArguments;


std::mutex tempLoaderLock;

boss::Expression applyEngine(Expression &&e, EvalFunction eval) {
  auto *r = new BOSSExpression{std::move(e)};
  auto *oldWrapper = r;
  r = eval(r);
  delete oldWrapper;
  auto result = std::move(r->delegate);
  delete r;
  return std::move(result);
}
  
template <typename T>
double averageDifference(const std::vector<T>& vec) {
    if (vec.size() < 2) {
        return 0.0;
    }

    size_t totalDifference = 0;
    for (size_t i = 1; i < vec.size(); ++i) {
        totalDifference += vec[i] - vec[i - 1];
    }

    double average = static_cast<double>(totalDifference) / (vec.size() - 1);
    return average;
}

template <typename T>
std::pair<std::vector<std::pair<T, T>>, std::vector<T>>
createAdjacentIntervalsAndNonAdjacents(const std::vector<T>& input) {
  std::vector<std::pair<T, T>> intervals;
  std::vector<T> nonAdjacent;

  if (input.empty()) {
    return {intervals, nonAdjacent};
  }

  intervals.reserve(input.size() / 2);
  nonAdjacent.reserve(input.size() / 2);

  T start = input[0];
  T prev = input[0];
  bool inInterval = false;
  
  for (size_t i = 1; i < input.size(); i++) {
    if (input[i] == prev + 1) {
      inInterval = true;
    } else {
      if (inInterval) {
	intervals.emplace_back(start, prev);
      } else {
	nonAdjacent.push_back(prev);
      }

      start = input[i];
      inInterval = false;
    }
    prev = input[i];
  }

  if (inInterval) {
    intervals.emplace_back(start, prev);
  } else {
    nonAdjacent.push_back(prev);
  }  
  
  return {intervals, nonAdjacent};
}

template <typename T>
std::vector<std::pair<T, T>> createEvenIntervals(const std::vector<T>& sortedValues, int32_t numRanges) {
  auto n = sortedValues.size();
  std::vector<std::pair<T, T>> result;
  result.reserve(numRanges);
  
  if (numRanges >= n) {
    for (size_t i = 0; i < n; i++) {
      result.emplace_back(sortedValues[i], sortedValues[i]);
    }
    return result;
  }

  size_t baseSize = n / numRanges;
  size_t remainder = n % numRanges;

  size_t startI = 0;
  for (size_t rangeI = 0; rangeI < numRanges; rangeI++) {
    size_t endI = startI + baseSize + (rangeI < remainder ? 1 : 0) - 1;
    result.emplace_back(sortedValues[startI], sortedValues[endI]);
    startI = endI + 1;
  }
  
  return std::move(result);
}

template <typename T>
std::vector<std::pair<T, T>> createMinimisedIntervals(const std::vector<T>& sortedValues, int32_t numRanges) {
  auto n = sortedValues.size();
  size_t mergeIndicator = std::numeric_limits<T>::max();
  std::vector<std::pair<T, T>> result;
  result.reserve(numRanges);

  if (numRanges >= n) {
    for (size_t i = 0; i < n; i++) {
      result.emplace_back(sortedValues[i], sortedValues[i]);
    }
    return result;
  }

  std::vector<size_t> differences;
  differences.reserve(n);
  for (size_t i = 1; i < n; i++) {
    auto diff = sortedValues[i] - sortedValues[i - 1];
    differences.push_back(diff);
  }

  auto numMergesNeeded = sortedValues.size() - numRanges;

  using Element = std::pair<T, size_t>;
  std::priority_queue<Element, std::vector<Element>, std::greater<Element>> minHeap;

  for (size_t i = 0; i < differences.size(); i++) {
    minHeap.push({differences[i], i});
  }

  for (size_t i = 0; i < numMergesNeeded; i++) {
    auto [minDiff, minIndex] = minHeap.top();
    minHeap.pop();

    differences[minIndex] = mergeIndicator;
  }

  bool merging = false;
  size_t currMergeStart = 0;
  size_t currMergeEnd = 0;

  for (size_t i = 0; i < differences.size(); i++) {
    if (differences[i] == mergeIndicator) {
      if (!merging) {
	currMergeStart = sortedValues[i];
	currMergeEnd = sortedValues[i + 1];
	merging = true;
      } else {
	currMergeEnd = sortedValues[i + 1];
      }
      if (i == differences.size() - 1) {
	result.emplace_back(currMergeStart, currMergeEnd);
      }
    } else {
      if (merging) {
	result.emplace_back(currMergeStart, currMergeEnd);
	merging = false;
      }
      if (i < differences.size() - 1 && differences[i + 1] != mergeIndicator) {
	result.emplace_back(sortedValues[i + 1], sortedValues[i + 1]);
      }
    }
  }

#ifdef DEBUG
  std::cout << "NUM RANGES REQUESTED: " << numRanges << " NUM RANGES PRODUCED: " << result.size() << std::endl;
#endif
  
  return std::move(result);
}

template <typename T>
std::vector<std::pair<T, T>> processIntervalsAsync(const std::vector<T>& sortedList, int32_t numRanges, int32_t numPartitions) {
  if (sortedList.size() == 0) {
    return {};
  }
  size_t n = sortedList.size();
  if (numPartitions > n) {
    numPartitions = n;
  }

  std::vector<std::future<std::vector<std::pair<T, T>>>> futures;
  std::vector<std::pair<T, T>> result;
  result.reserve(numRanges);

  size_t partitionSize = n / numPartitions;
  size_t remainder = n % numPartitions;

  size_t startI = 0;

  std::vector<std::vector<T>> partitions;
  partitions.reserve(numPartitions);
  std::vector<int32_t> partitionRanges;
  partitionRanges.reserve(numPartitions);

  for (size_t partitionI = 0; partitionI < numPartitions; partitionI++) {
    size_t endI = startI + partitionSize + (partitionI < remainder ? 1 : 0);

    std::vector<T> sortedPartitionList(sortedList.begin() + startI, sortedList.begin() + endI);
    int32_t partitionRange = static_cast<int32_t>((static_cast<double_t>(sortedPartitionList.size()) / n) * numRanges);
    if (partitionRange == 0) {
      partitionRange = 1;
    }
    partitions.emplace_back(std::move(sortedPartitionList));
    partitionRanges.emplace_back(std::move(partitionRange));
    startI = endI;
  }

  for (size_t partitionI = 0; partitionI < numPartitions; partitionI++) {
    futures.push_back(std::async(std::launch::async, [&partitions, &partitionRanges, partitionI]() mutable {
      return createMinimisedIntervals<T>(partitions[partitionI], partitionRanges[partitionI]);
    }));
  }

  for (auto& future : futures) {
    auto tempRes = future.get();
    result.insert(result.end(), std::make_move_iterator(tempRes.begin()), std::make_move_iterator(tempRes.end()));
  }

  return std::move(result);
}

boss::Expression
createFetchExpression(const std::string &url,
		      std::vector<int64_t> &bounds, bool trackingCache, int64_t numArgs) {
  auto numBounds = bounds.size();
  constexpr int64_t REQUEST_AIM = 128;
  ExpressionArguments args;

  int64_t padding = 0;
  int64_t alignment = 1024;
  int64_t ranges = CHANGEABLE_RANGES;
  int64_t requests = -1;
  int64_t numThreads = NUM_THREADS;
  if (!trackingCache) {
    padding = 0;
    alignment = 1;
    requests = -1;
  } else if (numBounds < 2 * ranges) {
    alignment = 1;
  }
  ranges = ranges / REQUEST_AIM;

#ifdef DEBUG
  std::cout << "PADDING: " << padding;
  std::cout << ", ALIGNMENT: " << alignment;
  std::cout << ", RANGES: " << ranges;
  std::cout << ", NUM BOUNDS: " << numBounds;
  std::cout << ", NUM THREADS: " << numThreads;
  std::cout << ", TRACKING CACHE: " << trackingCache;
  std::cout << ", REQUESTS: " << requests << std::endl;
#endif
  
  args.push_back(std::move("List"_(std::move(boss::Span<int64_t>(bounds)))));
  args.push_back(url);
  args.push_back(padding);
  args.push_back(alignment);
  args.push_back(ranges);
  args.push_back(requests);
  args.push_back(trackingCache);
  args.push_back(numThreads);

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
        "Wisent Deserialiser: No spans present in ByteSequence_ expression");
  }

  auto &typedSpan = spans[0];
  if (!std::holds_alternative<boss::Span<int8_t>>(typedSpan)) {
    throw std::runtime_error(
        "unsupported span type: ByteSequence span is not an int8_t span");
  }
  auto &byteSpan = std::get<boss::Span<int8_t>>(typedSpan);
  return std::move(byteSpan);
}


namespace boss::engines::WisentDeserialiser {
using EvalFunction = BOSSExpression *(*)(BOSSExpression *);
using std::move;
namespace utilities {} // namespace utilities

#ifdef DEBUG
template <typename T> void print_type_name() {
  const char *typeName = typeid(T).name();

#ifndef _MSC_VER
  // Demangle the type name on GCC/Clang
  int status = -1;
  std::unique_ptr<char, void (*)(void *)> res{
      abi::__cxa_demangle(typeName, nullptr, nullptr, &status), std::free};
  std::cout << (status == 0 ? res.get() : typeName) << std::endl;
#else
  // On MSVC, typeid().name() returns a human-readable name.
  std::cout << typeName << std::endl;
#endif
}
#endif

template <typename T>
T convertFromByteSpan(boss::Span<int8_t> &byteSpan, size_t start) {
  T res = 0;
  std::memcpy(&res, &(byteSpan[start]), sizeof(T));
  return res;
}

boss::Expression
deserialiseByteSequenceCopy(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "ByteSequence"_) {
    throw std::runtime_error(
        "Unsupported expression: deserialiseByteSequence function does not "
        "support deserialising from non-ByteSequence_ expressions");
  }

  if (spans.empty()) {
    throw std::runtime_error(
        "Wisent Deserialiser: No spans present in ByteSequence_ expression");
  }

  auto &typedSpan = spans[0];
  if (!std::holds_alternative<boss::Span<int8_t>>(typedSpan)) {
    throw std::runtime_error(
        "unsupported span type: ByteSequence span is not an int8_t span");
  }
  auto &byteSpan = std::get<boss::Span<int8_t>>(typedSpan);
  uint64_t const argumentCount = convertFromByteSpan<uint64_t>(byteSpan, 0);
  uint64_t const argumentBytesCount = convertFromByteSpan<uint64_t>(byteSpan, 8);
  uint64_t const expressionCount = convertFromByteSpan<uint64_t>(byteSpan, 16);
  uint64_t const argumentDictionaryBytesCount = convertFromByteSpan<uint64_t>(byteSpan, 24);
  uint64_t stringArgumentsFillIndex = convertFromByteSpan<size_t>(byteSpan, 32);

#ifdef DEBUG
  std::cout << "ARG COUNT: " << argumentCount << std::endl;
  std::cout << "EXPR COUNT: " << expressionCount << std::endl;
#endif

  size_t argumentsSize = byteSpan.size() - sizeof(RootExpression);

  RootExpression *root = [&]() -> RootExpression * {
    RootExpression *root =
        (RootExpression *) // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        malloc(sizeof(RootExpression) + argumentsSize);
    *((uint64_t *)&root
          ->argumentCount) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        argumentCount;
    *((uint64_t *)&root
          ->argumentBytesCount) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        argumentBytesCount;
    *((uint64_t *)&root
          ->expressionCount) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        expressionCount;
    *((uint64_t *)&root
          ->argumentDictionaryBytesCount) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        argumentDictionaryBytesCount;
    *((uint64_t *)&root
          ->stringArgumentsFillIndex) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        stringArgumentsFillIndex;
    *((void **)&root
          ->originalAddress) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        root;
    return root;
  }();
  memcpy(root->arguments, &byteSpan[sizeof(RootExpression)], argumentsSize);
  auto res = SerializedExpression(root).deserialize();
  return std::move(res);
}

boss::Expression
deserialiseByteSequenceMove(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "ByteSequence"_) {
    throw std::runtime_error(
        "Unsupported expression: deserialiseByteSequence function does not "
        "support deserialising from non-ByteSequence_ expressions");
  }

  if (spans.empty()) {
    throw std::runtime_error(
        "Wisent Deserialiser: No spans present in ByteSequence_ expression");
  }

  auto &typedSpan = spans[0];
  if (!std::holds_alternative<boss::Span<int8_t>>(typedSpan)) {
    throw std::runtime_error(
        "unsupported span type: ByteSequence span is not an int8_t span");
  }
  auto &byteSpan = std::get<boss::Span<int8_t>>(typedSpan);

#ifdef DEBUG
  std::cout << "NO BYTES: " << byteSpan.size() << std::endl;
#endif

  RootExpression *root = (RootExpression *)byteSpan.begin();
  *((void **)&root->originalAddress) = byteSpan.begin();

  auto res = SerializedExpression<nullptr, nullptr, nullptr>(root);
#ifdef DEBUG
  std::cout << "ARG COUNT: " << res.argumentCount() << std::endl;
  std::cout << "EXPR COUNT: " << res.expressionCount() << std::endl;
#endif
  return std::move(std::move(res).deserialize());
}

  boss::ComplexExpression mergeLists(boss::ComplexExpression &&listA,
				     boss::ComplexExpression &&listB) {
    auto [headA, unused_A, dynamicsA, spansA] = std::move(listA).decompose();
    auto [headB, unused_B, dynamicsB, spansB] = std::move(listB).decompose();

    std::transform(std::make_move_iterator(spansB.begin()),
                   std::make_move_iterator(spansB.end()),
                   std::back_inserter(spansA),
                   [](auto &&span) { return std::move(span); });

    return boss::ComplexExpression(std::move(headA), {}, {}, std::move(spansA));
  }
  
  boss::ComplexExpression mergeColumns(boss::ComplexExpression &&colA,
				       boss::ComplexExpression &&colB) {
    ExpressionArguments args;
    auto [headA, unused_A, dynamicsA, spansA] = std::move(colA).decompose();
    auto [headB, unused_B, dynamicsB, spansB] = std::move(colB).decompose();
    args.push_back(std::move(
        mergeLists(std::move(get<boss::ComplexExpression>(dynamicsA[0])),
                   std::move(get<boss::ComplexExpression>(dynamicsB[0])))));

    return boss::ComplexExpression(std::move(headA), {}, std::move(args), {});
  }
    
  boss::ComplexExpression mergeTables(boss::ComplexExpression &&tableA,
				      boss::ComplexExpression &&tableB) {
    ExpressionArguments args;
    auto [headA, unused_A, dynamicsA, spansA] = std::move(tableA).decompose();
    auto [headB, unused_B, dynamicsB, spansB] = std::move(tableB).decompose();
    
    auto itA = std::make_move_iterator(dynamicsA.begin());
    auto itB = std::make_move_iterator(dynamicsB.begin());
    for (; itA != std::make_move_iterator(dynamicsA.end()) &&
           itB != std::make_move_iterator(dynamicsB.end());
         ++itA, ++itB) {
      args.push_back(std::move(
			       mergeColumns(std::move(get<boss::ComplexExpression>(*itA)),
					    std::move(get<boss::ComplexExpression>(*itB)))));
    }
    
    return boss::ComplexExpression("Table"_, {}, std::move(args), {});
  }

  boss::ComplexExpression mergeTablesWithUniqueColumns(std::vector<ComplexExpression> &&tableExprs) {
    if (tableExprs.size() <= 0) {
      return "NoTablesToMerge"_();
    }
    if (tableExprs.size() == 1) {
      return std::move(tableExprs[0]);
    }
    auto resTable = std::move(tableExprs[0]);
    auto [resHead, unused_, resDynamics, resSpans] = std::move(resTable).decompose();

    for (auto it = std::make_move_iterator(std::next(tableExprs.begin()));
	 it != std::make_move_iterator(tableExprs.end()); ++it) {
      auto [currHead, currUnused_, currDynamics, currSpans] = std::move(*it).decompose();

      for (size_t colI = 0; colI < currDynamics.size(); colI++) {
	resDynamics.push_back(std::move(currDynamics[colI]));
      }
    }
    return boss::ComplexExpression(std::move(resHead), {}, std::move(resDynamics), std::move(resSpans));
  }

boss::Expression parseByteSequences(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  std::transform(std::make_move_iterator(dynamics.begin()),
                 std::make_move_iterator(dynamics.end()), dynamics.begin(),
                 [&](auto &&arg) {
                   return deserialiseByteSequenceMove(
                       std::move(get<boss::ComplexExpression>(arg)));
                 });  
  boss::ComplexExpression resTable = std::move(get<boss::ComplexExpression>(dynamics[0]));
  for (auto it = std::make_move_iterator(std::next(dynamics.begin()));
       it != std::make_move_iterator(dynamics.end()); ++it) {
    resTable =
        std::move(mergeTables(std::move(resTable),
                              std::move(get<boss::ComplexExpression>(*it))));
  }
  return std::move(resTable);
}
  
std::vector<boss::Symbol> getColumns(boss::ComplexExpression &&expression) {
  std::vector<boss::Symbol> columns;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "List"_) {
    return columns;
  }

  std::for_each(std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()),
                [&columns](auto &&arg) {
                  if (std::holds_alternative<boss::Symbol>(arg)) {
#ifdef DEBUG
                    std::cout << "SELECTED: " << arg << std::endl;
#endif
                    columns.emplace_back(std::get<boss::Symbol>(arg));
                  }
                });

  return std::move(columns);
}
  
  void parallelIotaSimd(std::vector<int64_t>& vec, int64_t start, unsigned numThreads = std::thread::hardware_concurrency()) {
    size_t n = vec.size();
    if (n == 0) return;
    if (numThreads == 0) numThreads = 1;

    size_t chunkSize = (n + numThreads - 1) / numThreads;
    std::vector<std::thread> threads;

    for (unsigned t = 0; t < numThreads; ++t) {
      size_t begin = t * chunkSize;
      size_t end = std::min(begin + chunkSize, n);

      threads.emplace_back([=, &vec]() {
	size_t i = begin;
	int64_t base = start + static_cast<int64_t>(begin);

#ifdef __AVX2__
	const size_t simdWidth = 4;
	__m256i increment = _mm256_set1_epi64x(simdWidth);
	__m256i val = _mm256_set_epi64x(base + 3, base + 2, base + 1, base);

	for (; i + simdWidth <= end; i += simdWidth) {
	  _mm256_storeu_si256(reinterpret_cast<__m256i*>(&vec[i]), val);
	  val = _mm256_add_epi64(val, increment);
	}
#endif
	for (; i < end; ++i) {
	  vec[i] = start + static_cast<int64_t>(i);
	}
      });
    }

    for (auto& thread : threads) thread.join();
  }
  
  boss::Expression addIndexColumnToTable(boss::ComplexExpression &&expression, boss::Symbol &&colHead = "__internal_indices_"_, int64_t numThreads = -1) {
    if (expression.getHead() != "Table"_) {
      return expression;
    }
    auto [head, unused_, dynamics, spans] = std::move(expression).decompose();

    bool containsIndices = false;
    std::for_each(dynamics.begin(), dynamics.end(), [&](auto const& expr) {
      if (std::holds_alternative<boss::ComplexExpression>(expr)) {
	containsIndices |= std::get<boss::ComplexExpression>(expr).getHead() == colHead;
      }
    });

    if (containsIndices) {
      auto tableExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
      return std::move(tableExpr);
    }


    auto const &tempSpanArgs = std::get<boss::ComplexExpression>(std::get<boss::ComplexExpression>(dynamics[0]).getDynamicArguments()[0]).getSpanArguments();
    auto numSpans = tempSpanArgs.size();
    ExpressionSpanArguments indices;
    
    if (numSpans > 1) {
      std::vector<size_t> spanOffsets(numSpans);
      std::vector<size_t> spanSizes(numSpans);

      spanOffsets[0] = 0;
      spanSizes[numSpans-1] = std::visit([&](auto const& typedSubSpan) {
	return typedSubSpan.size();
      }, tempSpanArgs[numSpans-1]);
      
      for (size_t i = 1; i < numSpans; i++) {
	auto const& subSpan = tempSpanArgs[i-1];
	std::visit([&](auto const& typedSubSpan) {
	  spanOffsets[i] = spanOffsets[i-1] + typedSubSpan.size();
	  spanSizes[i-1] = typedSubSpan.size();
	}, subSpan);
      }

      indices.resize(numSpans);
      size_t numAvailableThreads = numThreads < 0 ? 20 : numThreads;
      size_t numOuterThreads = numAvailableThreads < numSpans ? 1 : numSpans;
      size_t numInnerThreads = numAvailableThreads < numSpans ? numAvailableThreads : ((numAvailableThreads - numOuterThreads) / numOuterThreads);
#pragma omp parallel for num_threads(numOuterThreads)
      for (size_t i = 0; i < numSpans; i++) {
	const size_t n = spanSizes[i];
	const size_t offset = spanOffsets[i];

	std::vector<int32_t> indicesVector(n);
#pragma omp parallel for simd num_threads(numInnerThreads)
	for (size_t j = 0; j < n; j++) {
	  indicesVector[j] = offset + j;
	}
	indices[i] = std::move(boss::Span<int32_t>(std::move(indicesVector)));
      }
    } else {
      auto const& subSpan = tempSpanArgs[0];
      std::visit([&](auto const& typedSubSpan) {
	const int32_t n = static_cast<int32_t>(typedSubSpan.size());
	std::vector<int32_t> indicesVector(n);

	if (n > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(numThreads < 0 ? 20 : numThreads)
	  for (int32_t i = 0; i < n; i++) {
	    indicesVector[i] = i;
	  }
	} else if (n > SIMD_THRESHOLD) {
#pragma omp simd
	  for (int32_t i = 0; i < n; i++) {
	    indicesVector[i] = i;
	  }
	} else {
#pragma omp parallel for simd schedule(static) num_threads(numThreads < 0 ? 20 : numThreads)
	  for (int32_t i = 0; i < n; i++) {
	    indicesVector[i] = i;
	  }
	}
	
	indices.push_back(std::move(boss::Span<int32_t>(std::move(indicesVector))));
      }, subSpan);
    }
    
    auto listExpr = boss::ComplexExpression("List"_, {}, {}, std::move(indices));
    boss::ExpressionArguments colArgs;
    colArgs.push_back(std::move(listExpr));
    auto indexColExpr = boss::ComplexExpression(std::move(colHead), {}, std::move(colArgs), {});

    dynamics.push_back(std::move(indexColExpr));
    auto tableExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    return std::move(tableExpr);
  }

  boss::Expression addIndexColumnToTable(boss::ComplexExpression &&expression, ExpressionSpanArguments &&indices, boss::Symbol &&colHead = "__internal_indices_"_) {
    if (expression.getHead() != "Table"_) {
      return expression;
    }
    auto [head, unused_, dynamics, spans] = std::move(expression).decompose();

    bool containsIndices = false;
    std::for_each(dynamics.begin(), dynamics.end(), [&](auto const& expr) {
      if (std::holds_alternative<boss::ComplexExpression>(expr)) {
	containsIndices |= std::get<boss::ComplexExpression>(expr).getHead() == colHead;
      }
    });

    if (containsIndices) {
      auto tableExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
      return std::move(tableExpr);
    }
    
    auto numSpans = std::get<boss::ComplexExpression>(std::get<boss::ComplexExpression>(dynamics[0]).getDynamicArguments()[0]).getSpanArguments().size();

    auto listExpr = boss::ComplexExpression("List"_, {}, {}, std::move(indices));
    boss::ExpressionArguments colArgs;
    colArgs.push_back(std::move(listExpr));
    auto indexColExpr = boss::ComplexExpression(std::move(colHead), {}, std::move(colArgs), {});

    dynamics.push_back(std::move(indexColExpr));
    auto tableExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    return std::move(tableExpr);
  }

ExpressionSpanArguments
getExpressionSpanArgs(boss::ComplexExpression &&expression) {
  ExpressionSpanArguments spanArgs;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head == "Table"_) {
    if (dynamics.size() == 0) {
      std::vector<int64_t> filler = {};
      spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
      return spanArgs;
    }
    return getExpressionSpanArgs(
        std::move(std::get<boss::ComplexExpression>(std::move(dynamics[0]))));
  }
  std::for_each(std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()), [&](auto &&arg) {
                  auto [head, unused_, dynamics, spans] =
                      std::move(get<boss::ComplexExpression>(arg)).decompose();
                  std::for_each(std::make_move_iterator(spans.begin()),
                                std::make_move_iterator(spans.end()),
                                [&](auto &&span) {
                                  spanArgs.emplace_back(std::move(span));
                                });
                });
  return std::move(spanArgs);
}

ExpressionSpanArguments
getExpressionSpanArgs(boss::ComplexExpression &&expression,
                      boss::Symbol &&colHead) {
  ExpressionSpanArguments spanArgs;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head == "Table"_) {
    size_t idx = 0;
    for (; idx < dynamics.size(); idx++) {
      if (std::get<boss::ComplexExpression>(dynamics[idx]).getHead() ==
          colHead) {
        break;
      }
    }
    if (dynamics.size() == idx) {
      std::vector<int64_t> filler = {};
      spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
      return spanArgs;
    }
    return getExpressionSpanArgs(
        std::move(std::get<boss::ComplexExpression>(std::move(dynamics[idx]))));
  }
  std::for_each(std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()), [&](auto &&arg) {
                  auto [head, unused_, dynamics, spans] =
                      std::move(get<boss::ComplexExpression>(arg)).decompose();
                  std::for_each(std::make_move_iterator(spans.begin()),
                                std::make_move_iterator(spans.end()),
                                [&](auto &&span) {
                                  spanArgs.emplace_back(std::move(span));
                                });
                });
  return std::move(spanArgs);
}

ExpressionSpanArguments combineSpanArgs(ExpressionSpanArguments &&args1,
                                        ExpressionSpanArguments &&args2) {
  ExpressionSpanArguments alternatingArgs;
  if (args1.size() != args2.size()) {
    return std::move(alternatingArgs);
  }
  for (size_t i = 0; i < args1.size(); i++) {
    alternatingArgs.push_back(std::move(args1[i]));
    alternatingArgs.push_back(std::move(args2[i]));
  }

  return std::move(alternatingArgs);
}

ExpressionSpanArguments
getExpressionSpanArgsRanged(boss::ComplexExpression &&expression,
                            boss::Symbol &&startCol, boss::Symbol &&endCol) {
  ExpressionSpanArguments spanArgs;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head == "Table"_) {
    int64_t startIdx = -1;
    int64_t endIdx = -1;
    for (auto i = 0; i < dynamics.size(); i++) {
      if (std::get<boss::ComplexExpression>(dynamics[i]).getHead() ==
          startCol) {
        startIdx = i;
      }
      if (std::get<boss::ComplexExpression>(dynamics[i]).getHead() == endCol) {
        endIdx = i;
      }
    }
    if (startIdx == -1 || endIdx == -1 || startIdx == endIdx) {
      std::vector<int64_t> filler = {};
      spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
      return spanArgs;
    }
    auto startArgs = getExpressionSpanArgs(
        std::move(get<boss::ComplexExpression>(std::move(dynamics[startIdx]))));
    auto endArgs = getExpressionSpanArgs(
        std::move(get<boss::ComplexExpression>(std::move(dynamics[endIdx]))));
    return std::move(combineSpanArgs(std::move(startArgs), std::move(endArgs)));
  }
  std::vector<int64_t> filler = {};
  spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
  return spanArgs;
}
  
  ArgumentType Engine::getTypeOfColumn(LazilyDeserializedExpression &lazyExpr) {
    auto tempLazyChildExpr = lazyExpr[0];
    auto const &type = tempLazyChildExpr.getCurrentExpressionTypeExact();
      
    return type;
  }

template<typename T>
boss::expressions::ExpressionSpanArgument Engine::getSpanFromIndexRanges(
    LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
    IndicesManager<T> &indicesMan,
    const std::string &url, EvalFunction &loader, bool isSpan) requires IndicesInt<T> {

  auto exprOffsets = lazyExpr.expression();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
  auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
  auto const numChildren = endChildTypeOffset - startChildTypeOffset;
  
  auto const &type = getTypeOfColumn(lazyExpr);

  if (!tableMan.currColumnHasBeenLoaded()) {
    auto [startOffsets, sizes] = indicesMan.getArgOffsetsRanged(startChildOffset);
    tableMan.loadArgOffsetsContiguous(url, startOffsets, sizes, loader);
  }
#ifdef DEBUG
  std::cout << "RANGED TYPE: " << type << std::endl;
#endif
  if ((type == ArgumentType::ARGUMENT_TYPE_STRING ||
      type == ArgumentType::ARGUMENT_TYPE_SYMBOL) &&
      !tableMan.currColumnHasBeenLoaded()) {
    auto childStrOffsets = indicesMan.getStringOffsets(lazyExpr, numChildren, isSpan, tableMan);
    tableMan.loadStringOffsets(url, childStrOffsets, 26, loader);
  }
  if constexpr(std::is_same_v<T, int32_t>) {
    return std::move(spanFromRangeFunctors.at(type)(type, lazyExpr, indicesMan));
  } else if constexpr(std::is_same_v<T, int64_t>) {
    return std::move(spanFromRangeFunctors64.at(type)(type, lazyExpr, indicesMan));
  } else {
    static_assert(sizeof(T) == 0, "Unknown indices type for getting span from index ranges");
  }
}
FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_GET_SPAN_FROM_INDEX_RANGES_FOR);
  
  
template<typename T>
boss::expressions::ExpressionSpanArguments Engine::getSpanFromIndices(
    LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
    IndicesManager<T> &indicesMan,
    const std::string &url, EvalFunction &loader, bool isSpan,
    int64_t spanSize, int64_t numSpansOut, int64_t numThreads,
    boss::Symbol currColumn) requires IndicesInt<T> {

  auto exprOffsets = lazyExpr.expression();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
  auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
  auto const numArgs = endChildOffset - startChildOffset;
  auto const numChildren = endChildTypeOffset - startChildTypeOffset;

#ifdef DEBUG
  if (!indicesMan.indices.empty()) { 
    std::cout << "NUM INDICES: " << indicesMan.numIndices << std::endl;
  } else {
    std::cout << "EMPTY INDICES" << std::endl;
  }
#endif
  
  auto const &type = getTypeOfColumn(lazyExpr);

#ifdef DEBUG
  std::cout << "TYPE: " << static_cast<int32_t>(type) << std::endl;
#endif
  
  auto totalRanges = CHANGEABLE_RANGES;
  
#ifdef DEBUG
  std::cout << "TOTAL RANGES: " << totalRanges << std::endl;
#endif
  if (!tableMan.columnHasBeenLoaded(currColumn)) {
    auto argSizeAdjustedIndices = indicesMan.lazilyGetDeduplicatedIndices(type);
    if (indicesMan.useAsRanges) {
      tableMan.loadArgOffsetsRanged(url, *argSizeAdjustedIndices, loader);
    } else {
      tableMan.loadArgOffsets(url, *argSizeAdjustedIndices, loader, numThreads, startChildOffset, numChildren);
    }
  }
  
  if ((type == ArgumentType::ARGUMENT_TYPE_STRING ||
       type == ArgumentType::ARGUMENT_TYPE_SYMBOL) &&
      !tableMan.columnHasBeenLoaded(currColumn)) {
    auto childStrOffsets = indicesMan.getStringOffsets(lazyExpr, numChildren, isSpan, tableMan);
    tableMan.loadStringOffsets(url, childStrOffsets, 26, loader);
  }
  
  if constexpr(std::is_same_v<T, int32_t>) {
#ifdef DEBUG
    auto result = spanFunctors.at(type)(type, lazyExpr, indicesMan, numChildren, spanSize, numSpansOut, numThreads);
    size_t resultAmount = 0;
    for (auto const& spanArg : result) {
      std::visit([&](auto const& typedSpanArg) {
	resultAmount += typedSpanArg.size();
      }, spanArg);
    }
    assert(resultAmount == indicesMan.numIndices);
    return std::move(result);
#else
    return std::move(spanFunctors.at(type)(type, lazyExpr, indicesMan, numChildren, spanSize, numSpansOut, numThreads));
#endif
  } else if constexpr(std::is_same_v<T, int64_t>) {
#ifdef DEBUG
    auto result = spanFunctors64.at(type)(type, lazyExpr, indicesMan, numChildren, spanSize, numSpansOut, numThreads);
    size_t resultAmount = 0;
    for (auto const& spanArg : result) {
      std::visit([&](auto const& typedSpanArg) {
	resultAmount += typedSpanArg.size();
      }, spanArg);
    }
    assert(resultAmount == indicesMan.numIndices);
    return std::move(result);
#else
    return std::move(spanFunctors64.at(type)(type, lazyExpr, indicesMan, numChildren, spanSize, numSpansOut, numThreads));
#endif
  } else {
    static_assert(sizeof(T) == 0, "Unknown indices type for getSpanFromIndices");
  }
}
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_GET_SPAN_FROM_INDICES_FOR);
  

  template<typename T>
  boss::Expression Engine::decompressFOR(boss::ComplexExpression&& refsExpr, IndicesManager<T>& refsIndicesMan,
				 boss::ComplexExpression&& diffsExpr, IndicesManager<T>& diffsIndicesMan,
				 int32_t frameSize) requires IndicesInt<T> {
    auto [refHead, refUnused_, refDynamics, refUnused2_] =
      std::move(refsExpr).decompose();
    auto [diffHead, diffUnused_, diffDynamics, diffUnused2_] =
      std::move(diffsExpr).decompose();

    auto [refListHead, refListUnused_, refListUnused2_, refListSpans] =
      std::move(get<boss::ComplexExpression>(refDynamics[0])).decompose();
    auto [diffListHead, diffListUnused_, diffListUnused2_, diffListSpans] =
      std::move(get<boss::ComplexExpression>(diffDynamics[0])).decompose();
    
    auto refsSpan = get<boss::Span<int64_t>>(std::move(refListSpans[0]));
    auto diffsSpan = get<boss::Span<int32_t>>(std::move(diffListSpans[0]));

    if ((!refsIndicesMan.isSet && !diffsIndicesMan.isSet) ||
	(refsIndicesMan.indices.empty() && diffsIndicesMan.indices.empty())) {
      assert(diffsSpan.size() != 0);
      std::vector<int64_t> vals;
      vals.reserve(diffsSpan.size());
      for (int64_t refSpanI = 0, diffSpanI = 0;
	   refSpanI < refsSpan.size() && diffSpanI < diffsSpan.size();
	   refSpanI++, diffSpanI += frameSize) {
	auto& ref = refsSpan[refSpanI];
	for (int64_t inFrameI = 0;
	     inFrameI < frameSize && inFrameI + diffSpanI < diffsSpan.size();
	     inFrameI++) {
	  vals.push_back(ref + diffsSpan[inFrameI + diffSpanI]);
	}
      }
      return "List"_(boss::Span<int64_t>(std::move(vals)));
    }
    
    auto const& refsIndices = refsIndicesMan.indices;
    auto const& diffsIndices = diffsIndicesMan.indices;
    const size_t refN = refsIndices.size();
    const size_t diffN = diffsIndices.size();

    assert(diffsIndices.size() == diffsSpan.size());
    assert(refsIndices.size() == refsSpan.size());

    int64_t maxRefI = 0;
    if (refsIndicesMan.unsortedPossible) {
      maxRefI = *std::max_element(refsIndices.begin(), refsIndices.end());
    } else {
      maxRefI = refsIndices[refN - 1];
    }

    std::vector<int64_t> refIToSpanI(maxRefI + 1, -1);
    for (int64_t refSpanI = 0; refSpanI < refN; refSpanI++) {
      refIToSpanI[refsIndices[refSpanI]] = refsSpan[refSpanI];
    }
    
    std::vector<int64_t> vals(diffN);
    for (int64_t diffSpanI = 0; diffSpanI < diffN; diffSpanI++) {
      auto refI = static_cast<int64_t>(diffsIndices[diffSpanI] / frameSize);
      vals[diffSpanI] = refIToSpanI[refI] + diffsSpan[diffSpanI];
    }
    return "List"_(boss::Span<int64_t>(std::move(vals)));
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_DECOMPRESS_FOR_FOR);
  
  template<typename T>
  ExpressionSpanArguments Engine::deriveRLEValueIndicesFromRLEStarts(LazilyDeserializedExpression &lazyExpr,
							    TableManager &tableMan,
							    IndicesManager<T> &indicesMan,
							    const std::string &url,
							    EvalFunction &loader) requires IndicesInt<T> {
    auto exprOffsets = lazyExpr.expression();
    auto head = lazyExpr.getCurrentExpressionHead();

    assert(head == "RunStarts"_ || head == "List"_);
    
    auto const &startChildOffset = exprOffsets.startChildOffset;
    auto const &endChildOffset = exprOffsets.endChildOffset;
    auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
    auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
    auto const numArguments = endChildOffset - startChildOffset;
    auto const numChildren = endChildTypeOffset - startChildTypeOffset;
    
#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif

    if (head == "List"_) {
      ExpressionSpanArguments indicesSpanArgs;
      if (!indicesMan.isSet) {
	return indicesSpanArgs;
      }

      auto const& indices = indicesMan.indices;
      const size_t indicesN = indices.size();
      std::vector<int32_t> newIndices(indicesN);
      if (!indicesMan.unsortedPossible) {

	tableMan.loadArgOffsetsContiguous(url, startChildOffset, numArguments, loader);
	auto lazyChildExpr = lazyExpr[0];
	boss::Span<int32_t> starts = std::get<boss::Span<int32_t>>(lazyExpr.getCurrentExpressionAsSpanWithTypeAndSize(ArgumentType::ARGUMENT_TYPE_INT, numChildren));

	size_t startsI = 0;
	for (size_t indicesI = 0; indicesI < indicesN; indicesI++) {
	  auto iToFind = indices[indicesI];
	  while (startsI < numChildren && starts[startsI] < iToFind) {
	    startsI++;
	  }
	  newIndices[indicesI] = startsI - 1;
	}
      } else {
	bool binSearchCoversStarts = indicesMan.numIndices > (numArguments / std::log(numArguments));
	if (binSearchCoversStarts) {
	  tableMan.loadArgOffsetsContiguous(url, startChildOffset, numArguments, loader);
	}
	
	auto startsType = ArgumentType::ARGUMENT_TYPE_INT;
	size_t valsPerArg = sizeof(Argument) / sizeof(int32_t);
	std::vector<int32_t> starts(numChildren, -1);
      
	int64_t probeI = 0;
	int64_t probeS = 0;
	int64_t left = 0;
	int64_t right = numChildren - 1;
	auto lazyLeftExpr = lazyExpr(left / valsPerArg, left);
	auto lazyRightExpr = lazyExpr(right / valsPerArg, right);

	if (!binSearchCoversStarts) {
	  tableMan.loadArgOffsets<size_t>(url, {static_cast<size_t>(startChildOffset + (left / valsPerArg)), static_cast<size_t>(startChildOffset + (right / valsPerArg))}, loader, 1);
	}
	starts[left] = lazyLeftExpr.getCurrentExpressionInSpanAtAs<int32_t>(left);
	starts[right] = lazyRightExpr.getCurrentExpressionInSpanAtAs<int32_t>(right);
        
	for (auto i = 0; i < indicesN; i++) {
	  auto iToFind = indices[i];
	  while (left <= right) {
	    probeI = left + (right - left) / 2;
	    probeS = probeI + 1;

	    if (probeI == numChildren - 1) {
	      newIndices[i] = probeI;
	      break;
	    }
	  
	    if (!binSearchCoversStarts && (starts[probeI] == -1 || starts[probeS] == -1)) {
	      tableMan.loadArgOffsets<size_t>(url, {static_cast<size_t>(startChildOffset + (probeI / valsPerArg)), static_cast<size_t>(startChildOffset + (probeS / valsPerArg))}, loader, 1);
	    }
	    if (starts[probeI] == -1) {
	      auto lazyProbeIExpr = lazyExpr(probeI / valsPerArg, probeI);
	      starts[probeI] = lazyProbeIExpr.getCurrentExpressionInSpanAtAs<int32_t>(probeI);
	    }
	    if (starts[probeS] == -1) {
	      auto lazyProbeSExpr = lazyExpr(probeS / valsPerArg, probeS);
	      starts[probeS] = lazyProbeSExpr.getCurrentExpressionInSpanAtAs<int32_t>(probeS);
	    }
	  
	    if (starts[probeI] <= iToFind && starts[probeS] > iToFind) {
	      newIndices[i] = probeI;
	      break;
	    } else if (starts[probeS] <= iToFind) {
	      left = probeS;
	    } else if (starts[probeI] > iToFind) {
	      right = probeI;
	    }
	  }
	  assert(starts[probeI] <= iToFind && (probeS == numChildren || starts[probeS] > iToFind));
	  left = 0;
	  right = numChildren - 1;
	}
      }
      indicesSpanArgs.emplace_back(boss::Span<int32_t>(std::move(newIndices)));
      return std::move(indicesSpanArgs);
    } else {
      auto listOffset = 0;
      auto lazyListExpr = lazyExpr[listOffset];
      return deriveRLEValueIndicesFromRLEStarts(lazyListExpr, tableMan, indicesMan, url, loader);
    }
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_DERIVE_RLE_VALUE_INDICES_FROM_RLE_STARTS_FOR);
  

  template<typename T>
  boss::Expression Engine::lazyGatherRunLengthEncodedList(LazilyDeserializedExpression &lazyExpr,
							  TableManager &tableMan,
							  IndicesManager<T> &indicesMan,
							  const std::vector<boss::Symbol> &columns,
							  const std::string &url,
							  EvalFunction &loader) requires IndicesInt<T> {
    ExpressionArguments args;
    ExpressionSpanArguments spanArgs;

    auto exprOffsets = lazyExpr.expression();
    auto head = lazyExpr.getCurrentExpressionHead();

    auto const &startChildOffset = exprOffsets.startChildOffset;
    auto const &endChildOffset = exprOffsets.endChildOffset;
    auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
    auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
    auto const numArguments = endChildOffset - startChildOffset;
    auto const numChildren = endChildTypeOffset - startChildTypeOffset;
    
#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif
    bool isRLE = (head == "RLEList"_);
    if (isRLE) {
      auto startsOffset = 0;
      auto valsOffset = 1;

      auto lazyValsExpr = lazyExpr[valsOffset];
      auto lazyStartsExpr = lazyExpr[startsOffset];

      auto startsExprOffsets = lazyStartsExpr.expression();
      auto valsExprOffsets = lazyValsExpr.expression();

      auto startsListOffset = 0;
      auto const numStartsArguments = startsExprOffsets.endChildOffset - startsExprOffsets.startChildOffset;
      auto const numStartsChildren = startsExprOffsets.endChildTypeOffset - startsExprOffsets.startChildTypeOffset;
      tableMan.loadArgAndTypeOffsetsContiguous(url, startsExprOffsets.startChildOffset,
					       startsExprOffsets.startChildTypeOffset,
					       numStartsArguments, numStartsChildren, loader);
      auto lazyStartsListExpr = lazyStartsExpr[startsListOffset];
      
      ExpressionSpanArguments valsIndices =
	deriveRLEValueIndicesFromRLEStarts(lazyStartsListExpr, tableMan,
					   indicesMan, url, loader);
      IndicesManager<T> valsIndicesMan(std::move(valsIndices), false, tableMan.numRows, paramValsPerBit, indicesMan.duplicatesPossible, "RLE INDICES", true);
      
      auto valsExpr = lazyGather(lazyValsExpr, tableMan, valsIndicesMan,
				 columns, url, loader, -1, -1, "NONE"_, -1);
      auto [head, unused_, dynamics, spans] =
	std::move(get<boss::ComplexExpression>(valsExpr)).decompose();

      return std::move(dynamics[0]);
    }
    
    return boss::ComplexExpression(std::move(head), {}, std::move(args),
				   std::move(spanArgs));
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_LAZY_GATHER_RUN_LENGTH_ENCODED_LIST_FOR);
  
  template<typename T>
  boss::Expression Engine::lazyGatherFrameOfReferenceList(LazilyDeserializedExpression &lazyExpr,
							   TableManager &tableMan,
							   IndicesManager<T> &indicesMan,
							   const std::vector<boss::Symbol> &columns,
							   const std::string &url,
							   EvalFunction &loader) requires IndicesInt<T> {
    ExpressionArguments args;
    ExpressionSpanArguments spanArgs;

    auto exprOffsets = lazyExpr.expression();
    auto head = lazyExpr.getCurrentExpressionHead();

    auto const &startChildOffset = exprOffsets.startChildOffset;
    auto const &endChildOffset = exprOffsets.endChildOffset;
    auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
    auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
    auto const numArguments = endChildOffset - startChildOffset;
    auto const numChildren = endChildTypeOffset - startChildTypeOffset;
    
#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif
    bool isFOREnc = (head == "FORList"_);
    if (isFOREnc) {
      auto forSizeOffset = 0;
      auto spanSizeOffset = 1;
      auto refsOffset = 2;
      auto diffsOffset = 3;

      auto lazyFORSizeExpr = lazyExpr[forSizeOffset];
      auto lazySpanSizeExpr = lazyExpr[spanSizeOffset];
      auto lazyRefsExpr = lazyExpr[refsOffset];
      auto lazyDiffsExpr = lazyExpr[diffsOffset];

      int32_t frameSize = lazyFORSizeExpr.getCurrentExpressionAs<int32_t>();
      int64_t spanSize = lazySpanSizeExpr.getCurrentExpressionAs<int64_t>();
      auto refsExprOffsets = lazyRefsExpr.expression();
      auto diffsExprOffsets = lazyDiffsExpr.expression();

#ifdef DEBUG
      std::cout << "FRAME SIZE: " << frameSize << std::endl;
      std::cout << "SPAN SIZE: " << spanSize << std::endl;
#endif
      
      auto refsIndices = indicesMan.getFORConvertedIndicesAsExpressionSpanArguments(frameSize, spanSize, tableMan);
      IndicesManager<T> refsIndicesMan(std::move(refsIndices), false, tableMan.numRows, paramValsPerBit, false, "FOR REFS INDICES", indicesMan.unsortedPossible);
      auto refsExpr = lazyGather(lazyRefsExpr, tableMan, refsIndicesMan,
				 columns, url, loader, -1, -1, "NONE"_, -1);
      
      auto diffsExpr = lazyGather(lazyDiffsExpr, tableMan, indicesMan,
				  columns, url, loader, -1, -1, "NONE"_, -1);
      return decompressFOR(std::move(get<boss::ComplexExpression>(refsExpr)), refsIndicesMan,
			   std::move(get<boss::ComplexExpression>(diffsExpr)), indicesMan, frameSize);
    }
    
    return boss::ComplexExpression(std::move(head), {}, std::move(args),
				   std::move(spanArgs));
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_LAZY_GATHER_FRAME_OF_REFERENCE_LIST_FOR);


  template<typename T>
  boss::Expression Engine::lazyGatherBitPackedList(LazilyDeserializedExpression &lazyExpr,
							   TableManager &tableMan,
							   IndicesManager<T> &indicesMan,
							   const std::vector<boss::Symbol> &columns,
							   const std::string &url,
							   EvalFunction &loader) requires IndicesInt<T> {
    ExpressionArguments args;
    ExpressionSpanArguments spanArgs;

    auto exprOffsets = lazyExpr.expression();
    auto head = lazyExpr.getCurrentExpressionHead();

    auto const &startChildOffset = exprOffsets.startChildOffset;
    auto const &endChildOffset = exprOffsets.endChildOffset;
    auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
    auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
    auto const numArguments = endChildOffset - startChildOffset;
    auto const numChildren = endChildTypeOffset - startChildTypeOffset;
    
#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif
    bool isBitPacked = (head == "BitPackedList"_);
    if (isBitPacked) {
      auto bitsPerValOffset = 0;
      auto spanSizeOffset = 1;
      auto packedTypeOffset = 2;
      auto prevTypeOffset = 3;
      auto packedListOffset = 4;

      auto lazyBitsPerValExpr = lazyExpr[bitsPerValOffset];
      auto lazySpanSizeExpr = lazyExpr[spanSizeOffset];
      auto lazyPackedTypeExpr = lazyExpr[packedTypeOffset];
      auto lazyPackedListExpr = lazyExpr[packedListOffset];
      auto lazyPrevTypeExpr = lazyExpr[prevTypeOffset];
      int32_t bitsPerVal = lazyBitsPerValExpr.getCurrentExpressionAs<int32_t>();
      int64_t spanSize = lazySpanSizeExpr.getCurrentExpressionAs<int64_t>();
      ArgumentType packedType = static_cast<ArgumentType>(lazyPackedTypeExpr.getCurrentExpressionAs<int32_t>());
      ArgumentType prevType = static_cast<ArgumentType>(lazyPrevTypeExpr.getCurrentExpressionAs<int32_t>());
      
#ifdef DEBUG
      std::cout << "BIT PER VAL: " << bitsPerVal << std::endl;
      std::cout << "SPAN SIZE: " << spanSize << std::endl;
      std::cout << "PREV TYPE: " << static_cast<int32_t>(prevType) << std::endl;
      std::cout << "PACKED TYPE: " << static_cast<int32_t>(packedType) << std::endl;
#endif
      
      auto packedListExprOffsets = lazyPackedListExpr.expression();
      
      auto packedIndices = indicesMan.getBitPackedConvertedIndicesAsExpressionSpanArguments(bitsPerVal, packedType);
      IndicesManager<T> packedIndicesMan(std::move(packedIndices), false, tableMan.numRows, paramValsPerBit, true, "BIT PACKED INDICES", indicesMan.unsortedPossible);
      assert(packedIndicesMan.indices.size() == indicesMan.indices.size());
      auto packedListExpr = lazyGather(lazyPackedListExpr, tableMan, packedIndicesMan,
				       columns, url, loader, -1, -1, "NONE"_, -1);
      
      auto [pHead, pUnused_, pDynamics, pUnused2_] =
	std::move(get<boss::ComplexExpression>(packedListExpr)).decompose();
      auto [lHead, lUnused_, lUnused2_, lSpans] =
	std::move(get<boss::ComplexExpression>(pDynamics[0])).decompose();

      auto values = std::move(lSpans);
      const size_t indicesN = indicesMan.isSet ? indicesMan.indices.size() : spanSize;

      int32_t bitsPerUnit = indicesMan.getArgSizeFromType(packedType) * 8;
      int32_t valuesPerUnit = bitsPerUnit / bitsPerVal;

#ifdef DEBUG
      std::cout << "BITS PER UNIT: " << bitsPerUnit << ", VALS PER UNIT: " << valuesPerUnit << std::endl;
#endif

      std::vector<int32_t> result(indicesN);
      auto const& spanArg = values[0];
      std::visit([&](const auto& typedSpan) {
	size_t size = typedSpan.size();
	assert(static_cast<size_t>(std::ceil(static_cast<double>(spanSize) / valuesPerUnit)) >= size);
	auto const& arg0 = typedSpan[0];
	using ArgType = std::decay_t<decltype(arg0)>;
	if constexpr(std::is_same_v<ArgType, int8_t> ||
		     std::is_same_v<ArgType, int16_t> ||
		     std::is_same_v<ArgType, int32_t> ||
		     std::is_same_v<ArgType, int64_t>) {
	  ArgType mask = (static_cast<ArgType>(1) << bitsPerVal) - 1;
	  if (indicesMan.isSet) {
	    assert(indicesMan.indices.size() == packedIndicesMan.indices.size());
	    assert(indicesMan.indices.size() == size);
	    for (size_t i = 0; i < indicesN; i++) {
	      auto const& index = indicesMan.indices[i];
	      auto unit = static_cast<std::make_unsigned_t<ArgType>>(typedSpan[i]);
	      size_t offsetInUnit = index % valuesPerUnit;
	      size_t bitOffset = offsetInUnit * bitsPerVal;

	      int32_t value = static_cast<int32_t>((unit >> bitOffset) & mask);
	      result[i] = value;
	    }
	  } else {
	    size_t resI = 0;
	    for (size_t i = 0; i < size && resI < indicesN; i++) {
	      auto const& unit = typedSpan[i];
	      for (size_t j = 0; j < valuesPerUnit && resI < indicesN; j++, resI++) {
		size_t bitOffset = j * bitsPerVal;
		result[resI] = static_cast<int32_t>((unit >> bitOffset) & mask);
	      }
	    }
	    assert(resI == indicesN);
	    assert(result.size() == spanSize);
	  }
	}
      }, spanArg);
      
      ExpressionSpanArguments unpackedSpanArgs;
      unpackedSpanArgs.push_back(std::move(boss::Span<int32_t>(std::move(result))));
      return boss::ComplexExpression("List"_, {}, {}, std::move(unpackedSpanArgs));
    }
    
    return boss::ComplexExpression(std::move(head), {}, std::move(args),
				   std::move(spanArgs));
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_LAZY_GATHER_BIT_PACKED_LIST_FOR);
  
  template<typename T>
  boss::Expression Engine::lazyGatherSingleValueList(LazilyDeserializedExpression &lazyExpr,
						     TableManager &tableMan,
						     IndicesManager<T> &indicesMan,
						     const std::vector<boss::Symbol> &columns,
						     const std::string &url,
						     EvalFunction &loader,
						     int64_t spanSizeOut, int64_t numSpansOut,
						     int64_t numThreads) requires IndicesInt<T> {
    if (spanSizeOut > 0 && numSpansOut > 0) {
      throw std::runtime_error("Cannot call lazyGatherSingleValueList with non-zero spanSizeOut and numSpansOut");
    }
    
    ExpressionArguments args;
    ExpressionSpanArguments spanArgs;

    auto exprOffsets = lazyExpr.expression();
    auto head = lazyExpr.getCurrentExpressionHead();

    auto const &startChildOffset = exprOffsets.startChildOffset;
    auto const &endChildOffset = exprOffsets.endChildOffset;
    auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
    auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
    auto const numArguments = endChildOffset - startChildOffset;
    auto const numChildren = endChildTypeOffset - startChildTypeOffset;
    
#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif
    bool isSingleValue = (head == "SingleValueList"_);
    if (isSingleValue) {
      auto bitsPerValOffset = 0;
      auto spanSizeOffset = 1;
      auto packedTypeOffset = 2;
      auto singleValueListOffset = 3;

      auto lazyBitsPerValExpr = lazyExpr[bitsPerValOffset];
      auto lazySpanSizeExpr = lazyExpr[spanSizeOffset];
      auto lazyPackedTypeExpr = lazyExpr[packedTypeOffset];
      auto lazySingleValueListExpr = lazyExpr[singleValueListOffset];

      int32_t bitsPerVal = lazyBitsPerValExpr.getCurrentExpressionAs<int32_t>();
      int64_t spanSize = lazySpanSizeExpr.getCurrentExpressionAs<int64_t>();
      ArgumentType packedType = static_cast<ArgumentType>(lazyPackedTypeExpr.getCurrentExpressionAs<int32_t>());

#ifdef DEBUG
      std::cout << "BIT PER VAL: " << bitsPerVal << std::endl;
      std::cout << "SPAN SIZE: " << spanSize << std::endl;
      std::cout << "PACKED TYPE: " << packedType << std::endl;
#endif
      int32_t singleValue = 0;
      if (packedType == ArgumentType::ARGUMENT_TYPE_CHAR) {
	singleValue = static_cast<int32_t>(lazySingleValueListExpr.getCurrentExpressionAs<int8_t>());
      } else if (packedType == ArgumentType::ARGUMENT_TYPE_SHORT) {
	singleValue = static_cast<int32_t>(lazySingleValueListExpr.getCurrentExpressionAs<int16_t>());
      } else if (packedType == ArgumentType::ARGUMENT_TYPE_INT) {
	singleValue = static_cast<int32_t>(lazySingleValueListExpr.getCurrentExpressionAs<int32_t>());
      } else {
	throw std::runtime_error("INVALID PACKED TYPE FOUND FOR SINGLE VALUE LIST");
      }
      
#ifdef DEBUG
      std::cout << "SINGLE VALUE: " << singleValue << std::endl;
#endif
      const size_t indicesN = indicesMan.isSet ? indicesMan.indices.size() : spanSize;

      if (spanSizeOut <= 0 && numSpansOut <= 0) {
	spanSizeOut = indicesN;
	numSpansOut = 1;
      } else if (spanSizeOut <= 0 && numSpansOut > 0) {
	size_t nPerSpan = (indicesN / numSpansOut) + 1;
	spanSizeOut = nPerSpan;
      } else if (numSpansOut <= 0 && spanSizeOut > 0) {
	size_t numSpansUnderEst = indicesN / spanSizeOut;
	size_t remainder = indicesN % spanSizeOut;
	numSpansOut = numSpansUnderEst + (remainder == 0 ? 0 : 1);
      }

      ExpressionSpanArguments unpackedSpanArgs;
      unpackedSpanArgs.reserve(numSpansOut);
      for (size_t spanI = 0; spanI < indicesN; spanI += spanSizeOut) {
	size_t currSize = spanSizeOut < (indicesN - spanI) ? spanSizeOut : (indicesN - spanI);
	
	std::vector<int64_t> result(currSize);
#pragma omp parallel for schedule(static) num_threads(numThreads)
	for (size_t i = 0; i < currSize; i++) {
	  result[i] = singleValue;
	}
	unpackedSpanArgs.push_back(std::move(boss::Span<int64_t>(std::move(result))));
      }

      return boss::ComplexExpression("List"_, {}, {}, std::move(unpackedSpanArgs));
    }
    
    return boss::ComplexExpression(std::move(head), {}, std::move(args),
				   std::move(spanArgs));
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_LAZY_GATHER_SINGLE_VALUE_LIST_FOR);

  template<typename T>
  boss::Expression Engine::lazyGatherSequenceEncodedList(LazilyDeserializedExpression &lazyExpr,
							 TableManager &tableMan,
							 IndicesManager<T> &indicesMan,
							 const std::vector<boss::Symbol> &columns,
							 const std::string &url,
							 EvalFunction &loader,
							 int64_t spanSizeOut, int64_t numSpansOut,
							 int64_t numThreads) requires IndicesInt<T> {
    if (spanSizeOut > 0 && numSpansOut > 0) {
      throw std::runtime_error("Cannot call lazyGatherSequenceEncodedList with non-zero spanSizeOut and numSpansOut");
    }
    
    ExpressionArguments args;
    ExpressionSpanArguments spanArgs;

    auto exprOffsets = lazyExpr.expression();
    auto head = lazyExpr.getCurrentExpressionHead();

    auto const &startChildOffset = exprOffsets.startChildOffset;
    auto const &endChildOffset = exprOffsets.endChildOffset;
    auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
    auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
    auto const numArguments = endChildOffset - startChildOffset;
    auto const numChildren = endChildTypeOffset - startChildTypeOffset;
    
#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif
    bool isSequenceEncoded = (head == "SequenceEncodedList"_);
    if (isSequenceEncoded) {
      auto startOffset = 0;
      auto diffOffset = 1;
      auto spanSizeOffset = 2;

      auto lazyStartExpr = lazyExpr[startOffset];
      auto lazyDiffExpr = lazyExpr[diffOffset];
      auto lazySpanSizeExpr = lazyExpr[spanSizeOffset];

      ArgumentType startType = lazyStartExpr.getCurrentExpressionTypeExact();
      ArgumentType diffType = lazyDiffExpr.getCurrentExpressionTypeExact();
      
#ifdef DEBUG
      std::cout << "START TYPE: " << startType << std::endl;
      std::cout << "DIFF TYPE: " << diffType << std::endl;
#endif
      
      int64_t start = 0;
      if (startType == ArgumentType::ARGUMENT_TYPE_CHAR) {
	start = static_cast<int64_t>(lazyStartExpr.getCurrentExpressionAs<int8_t>());
      } else if (startType == ArgumentType::ARGUMENT_TYPE_SHORT) {
	start = static_cast<int64_t>(lazyStartExpr.getCurrentExpressionAs<int16_t>());
      } else if (startType == ArgumentType::ARGUMENT_TYPE_INT) {
	start = static_cast<int64_t>(lazyStartExpr.getCurrentExpressionAs<int32_t>());
      } else if (startType == ArgumentType::ARGUMENT_TYPE_LONG) {
	start = static_cast<int64_t>(lazyStartExpr.getCurrentExpressionAs<int64_t>());
      } else {
	throw std::runtime_error("INVALID TYPE FOUND IN SEQUENCE ENCODED LIST");
      }
      
      int64_t diff = 0;
      if (diffType == ArgumentType::ARGUMENT_TYPE_CHAR) {
	diff = static_cast<int64_t>(lazyDiffExpr.getCurrentExpressionAs<int8_t>());
      } else if (diffType == ArgumentType::ARGUMENT_TYPE_SHORT) {
	diff = static_cast<int64_t>(lazyDiffExpr.getCurrentExpressionAs<int16_t>());
      } else if (diffType == ArgumentType::ARGUMENT_TYPE_INT) {
	diff = static_cast<int64_t>(lazyDiffExpr.getCurrentExpressionAs<int32_t>());
      } else if (diffType == ArgumentType::ARGUMENT_TYPE_LONG) {
	diff = static_cast<int64_t>(lazyDiffExpr.getCurrentExpressionAs<int64_t>());
      } else {
	throw std::runtime_error("INVALID TYPE FOUND IN SEQUENCE ENCODED LIST");
      }

      int64_t spanSize = lazySpanSizeExpr.getCurrentExpressionAs<int64_t>();
      
#ifdef DEBUG
      std::cout << "START: " << start << std::endl;
      std::cout << "DIFF: " << diff << std::endl;
      std::cout << "SPAN SIZE: " << spanSize << std::endl;
#endif

      const size_t indicesN = indicesMan.isSet ? indicesMan.indices.size() : spanSize;
      
      if (spanSizeOut <= 0 && numSpansOut <= 0) {
	spanSizeOut = indicesN;
	numSpansOut = 1;
      } else if (spanSizeOut <= 0 && numSpansOut > 0) {
	size_t nPerSpan = (indicesN / numSpansOut) + 1;
	spanSizeOut = nPerSpan;
      } else if (numSpansOut <= 0 && spanSizeOut > 0) {
	size_t numSpansUnderEst = indicesN / spanSizeOut;
	size_t remainder = indicesN % spanSizeOut;
	numSpansOut = numSpansUnderEst + (remainder == 0 ? 0 : 1);
      }
      ExpressionSpanArguments unpackedSpanArgs;
      unpackedSpanArgs.reserve(numSpansOut);

      size_t tempI = 0;

      if (indicesN == spanSize) {
	for (size_t spanI = 0; spanI < indicesN; spanI += spanSizeOut) {
	  size_t currSize = spanSizeOut < (indicesN - spanI) ? spanSizeOut : (indicesN - spanI);
	  
	  std::vector<int64_t> result(currSize);
#pragma omp parallel for schedule(static) num_threads(numThreads)
	  for (size_t i = 0; i < currSize; i++) {
	    size_t tempILocal = spanI + i;
	    result[i] = start + tempILocal * diff;
	  }
	  unpackedSpanArgs.push_back(std::move(boss::Span<int64_t>(std::move(result))));
	}
      } else {
	for (size_t spanI = 0; spanI < indicesN; spanI += spanSizeOut) {
	  size_t currSize = spanSizeOut < (indicesN - spanI) ? spanSizeOut : (indicesN - spanI);
	
	  std::vector<int64_t> result(currSize);
#pragma omp parallel for schedule(static) num_threads(numThreads)
	  for (size_t i = 0; i < currSize; i++) {
	    size_t tempILocal = spanI + i;
	    result[i] = start + indicesMan.indices[tempILocal] * diff;
	  }
	  unpackedSpanArgs.push_back(std::move(boss::Span<int64_t>(std::move(result))));
	}
      }

      return boss::ComplexExpression("List"_, {}, {}, std::move(unpackedSpanArgs));
    }
    
    return boss::ComplexExpression(std::move(head), {}, std::move(args),
				   std::move(spanArgs));
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_LAZY_GATHER_SEQUENCE_ENCODED_LIST_FOR);

  template<typename T>
  boss::Expression Engine::lazyGatherDictionaryEncodedList(LazilyDeserializedExpression &lazyExpr,
							   TableManager &tableMan,
							   IndicesManager<T> &indicesMan,
							   const std::vector<boss::Symbol> &columns,
							   const std::string &url,
							   EvalFunction &loader,
							   int64_t spanSizeOut, int64_t numSpansOut,
							   boss::Symbol currColumn, int64_t numThreads) requires IndicesInt<T> {
    if (spanSizeOut > 0 && numSpansOut > 0) {
      throw std::runtime_error("Cannot call lazyGatherDictionaryEncodedList with non-zero spanSizeOut and numSpansOut");
    }

    ExpressionArguments args;
    ExpressionSpanArguments spanArgs;

    auto exprOffsets = lazyExpr.expression();
    auto head = lazyExpr.getCurrentExpressionHead();

    auto const &startChildOffset = exprOffsets.startChildOffset;
    auto const &endChildOffset = exprOffsets.endChildOffset;
    auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
    auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
    auto const numArguments = endChildOffset - startChildOffset;
    auto const numChildren = endChildTypeOffset - startChildTypeOffset;
    
#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif
    bool isDictEnc = (head == "DictionaryEncodedList"_);
    if (isDictEnc) {
      auto dictOffset = 0;
      auto offsetsOffset = 1;

      auto lazyOffsetsExpr = lazyExpr[offsetsOffset];
      auto lazyDictExpr = lazyExpr[dictOffset];

      if (tableMan.isInitialRun) {
	ExpressionSpanArguments tempArgs;
	IndicesManager<T> emptyDictIndicesMan(std::move(tempArgs), false, -1, -1, true, "EMPTY DICT INDICES", true);
	auto dictExpr = lazyGather(lazyDictExpr, tableMan, emptyDictIndicesMan,
				   columns, url, loader, -1, -1, currColumn, numThreads);
	tableMan.dictionaryMap[currColumn] = std::move(dictExpr);
	bool justCachingDicts = std::find(columns.begin(), columns.end(),
					  currColumn) == columns.end();
	if (justCachingDicts) {
	  return "DictCached"_;
	}
      }
      
      auto offsetsExpr = lazyGather(lazyOffsetsExpr, tableMan, indicesMan,
				    columns, url, loader, -1, -1, currColumn, numThreads);
      auto [oHead, oUnused_, oDynamics, oUnused2_] =
	std::move(get<boss::ComplexExpression>(offsetsExpr)).decompose();
      auto [lHead, lUnused_, lUnused2_, lSpans] =
	std::move(get<boss::ComplexExpression>(oDynamics[0])).decompose();
      
      auto indices = std::move(lSpans); 
      auto& dictExpr = tableMan.dictionaryMap[currColumn];
      auto& dictDynamics = std::get<boss::ComplexExpression>(dictExpr).getDynamicArguments();
      auto& dictListExpr = dictDynamics[0];
      auto& dlSpans = std::get<boss::ComplexExpression>(dictListExpr).getSpanArguments();
      const auto& dictSpan = dlSpans[0];
      const auto& offsetsSpan = indices[0];

      ExpressionSpanArguments dictSpanArgs;

      std::visit([&](const auto& typedOffsets) {
	const size_t numValues = typedOffsets.size();
	const auto& offset0 = typedOffsets[0];
	using OffsetType = std::decay_t<decltype(offset0)>;
	if constexpr(std::is_same_v<OffsetType, int8_t> ||
		     std::is_same_v<OffsetType, int16_t> ||
		     std::is_same_v<OffsetType, int32_t> ||
		     std::is_same_v<OffsetType, int64_t>) {
	  std::visit([&](const auto& typedDict) {
	    const size_t size = typedDict.size();
	    const auto& arg0 = typedDict[0];
	    using ArgType = std::decay_t<decltype(arg0)>;
	    if constexpr(std::is_same_v<ArgType, std::string>) {
	      if (spanSizeOut <= 0 && numSpansOut <= 0) {
		spanSizeOut = numValues;
		numSpansOut = 1;
	      } else if (spanSizeOut <= 0 && numSpansOut > 0) {
		size_t nPerSpan = (numValues / numSpansOut) + 1;
		spanSizeOut = nPerSpan;
	      } else if (numSpansOut <= 0 && spanSizeOut > 0) {
		size_t numSpansUnderEst = numValues / spanSizeOut;
		size_t remainder = numValues % spanSizeOut;
		numSpansOut = numSpansUnderEst + (remainder == 0 ? 0 : 1);
	      }

	      size_t tempI = 0;

	      for (size_t spanI = 0; spanI < numValues; spanI += spanSizeOut) {
		size_t currSize = spanSizeOut < (numValues - spanI) ? spanSizeOut : (numValues - spanI);
		std::vector<OffsetType> res(currSize);
#pragma omp parallel for schedule(static) num_threads(numThreads)
		for (size_t i = 0; i < currSize; i++) {
		  size_t tempILocal = spanI + i;
		  assert(tempILocal < numValues);
		  assert(typedOffsets[tempILocal] < size);
		  res[i] = typedOffsets[tempILocal];
		}
		dictSpanArgs.push_back(std::move(boss::Span<OffsetType>(std::move(res))));	      
	      }
	    }
	    else if constexpr(std::is_same_v<ArgType, int8_t> ||
			      std::is_same_v<ArgType, int16_t> ||
			      std::is_same_v<ArgType, int32_t> ||
			      std::is_same_v<ArgType, int64_t> ||
			      std::is_same_v<ArgType, float_t> ||
			      std::is_same_v<ArgType, double_t> ||
			      std::is_same_v<ArgType, bool>) {
	      
	      if (spanSizeOut <= 0 && numSpansOut <= 0) {
		spanSizeOut = numValues;
		numSpansOut = 1;
	      } else if (spanSizeOut <= 0 && numSpansOut > 0) {
		size_t nPerSpan = (numValues / numSpansOut) + 1;
		spanSizeOut = nPerSpan;
	      } else if (numSpansOut <= 0 && spanSizeOut > 0) {
		size_t numSpansUnderEst = numValues / spanSizeOut;
		size_t remainder = numValues % spanSizeOut;
		numSpansOut = numSpansUnderEst + (remainder == 0 ? 0 : 1);
	      }

	      size_t tempI = 0;
	      for (size_t spanI = 0; spanI < numValues; spanI += spanSizeOut) {
		size_t currSize = spanSizeOut < (numValues - spanI) ? spanSizeOut : (numValues - spanI);
		std::vector<ArgType> res(currSize);
#pragma omp parallel for schedule(static) num_threads(numThreads)
		for (size_t i = 0; i < currSize; i++) {
		  size_t tempILocal = spanI + i;
		  assert(tempILocal < numValues);
		  assert(typedOffsets[tempILocal] < size);
		  res[i] = typedDict[typedOffsets[tempILocal]];
		}
		dictSpanArgs.push_back(std::move(boss::Span<ArgType>(std::move(res))));	      
	      }
	    } else {
	      throw std::runtime_error("Unknown dict type in lazy gather dict");
	    }
	  }, dictSpan);
	} else {
	  throw std::runtime_error("Unknown offset type in lazy gather dict");
	}
      }, offsetsSpan);

      return boss::ComplexExpression("List"_, {}, {}, std::move(dictSpanArgs));
    }
    
    return boss::ComplexExpression(std::move(head), {}, std::move(args),
				   std::move(spanArgs));
  }
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_LAZY_GATHER_DICTIONARY_ENCODED_LIST_FOR);

  template<typename T>
  boss::Expression Engine::lazyGather(LazilyDeserializedExpression &lazyExpr,
				      TableManager &tableMan,
				      IndicesManager<T> &indicesMan,
				      const std::vector<boss::Symbol> &columns,
				      const std::string &url,
				      EvalFunction &loader,
				      int64_t spanSizeOut, int64_t numSpansOut,
				      boss::Symbol currColumn, int64_t numThreads) requires IndicesInt<T> {
  ExpressionArguments args;
  ExpressionSpanArguments spanArgs;

  auto exprOffsets = lazyExpr.expression();
  auto head = lazyExpr.getCurrentExpressionHead();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const &startChildTypeOffset = exprOffsets.startChildTypeOffset;
  auto const &endChildTypeOffset = exprOffsets.endChildTypeOffset;
  auto const numArguments = endChildOffset - startChildOffset;
  auto const numChildren = endChildTypeOffset - startChildTypeOffset;

#ifdef DEBUG
    std::cout << "HEAD: " << head << ", START CHILD OFFSET: " << startChildOffset << ", END CHILD OFFSET: " << endChildOffset << ", START CHILD TYPE OFFSET: " << startChildTypeOffset << ", END CHILD TYPE OFFSET: " << endChildTypeOffset << std::endl;
#endif
  
  bool isSpan = (head == "List"_);
#ifdef DEBUG
  std::cout << "IS SPAN: " << isSpan << std::endl;
#endif
  if (isSpan) {
    indicesMan.checkIsValid(numChildren, 0.6);
    if (indicesMan.isSet && indicesMan.isValid) {
      if (!indicesMan.isRanged) {
        spanArgs = std::move(getSpanFromIndices(lazyExpr, tableMan, indicesMan, url, loader, isSpan, spanSizeOut, numSpansOut, numThreads, currColumn));
      } else {
        spanArgs.push_back(std::move(getSpanFromIndexRanges(
           lazyExpr, tableMan, indicesMan, url, loader, isSpan)));
      }
    } else {
      // Assumes same type for all spans and that all children are spans
      
      auto const &type = getTypeOfColumn(lazyExpr);
      if (!tableMan.columnHasBeenLoaded(currColumn)) {
	tableMan.loadArgOffsetsContiguous(url, startChildOffset, numArguments, loader);
      }
      
#ifdef DEBUG
      std::cout << "TYPE: " << static_cast<int32_t>(type) << std::endl;
#endif
      if ((type == ArgumentType::ARGUMENT_TYPE_STRING ||
	   type == ArgumentType::ARGUMENT_TYPE_SYMBOL) &&
	  !tableMan.columnHasBeenLoaded(currColumn)) {
	std::vector<size_t> childStrOffsets = lazyExpr.getCurrentExpressionAsStringOffsetsVector(numChildren);
	tableMan.loadStringOffsets(url, childStrOffsets, 26, loader);
      }
      if (indicesMan.isSet) {
	if constexpr(std::is_same_v<T, int32_t>) {
	  spanArgs = std::move(spanFunctors.at(type)(type, lazyExpr, indicesMan, numChildren, spanSizeOut, numSpansOut, numThreads));
	} else if constexpr(std::is_same_v<T, int64_t>) {
	  spanArgs = std::move(spanFunctors64.at(type)(type, lazyExpr, indicesMan, numChildren, spanSizeOut, numSpansOut, numThreads));
	}
      } else {
	auto tempSpanArgs = lazyExpr.getCurrentExpressionAsSpanWithTypeAndSize(type, numChildren, spanSizeOut, numSpansOut);
        spanArgs = std::move(tempSpanArgs);
      }
    }
  } else {

    for (auto offset = 0; offset < numChildren; offset++) {

      auto lazyChildExpr = lazyExpr[offset];
      if (lazyChildExpr.currentIsExpression()) {

	auto childHead = lazyChildExpr.getCurrentExpressionHead();

        if (head == "Table"_) {
          auto childIsGathered =
              std::find(columns.begin(), columns.end(),
                        childHead) !=
              columns.end();
	  auto childWillBeGathered =
	    tableMan.allSymbols.find(childHead) != tableMan.allSymbols.end();
#ifdef DEBUG
	  if (childIsGathered || (childWillBeGathered && tableMan.isInitialRun)) {
	    std::cout << "CHILD HEAD: "
		      << childHead
		      << " IS GATHERED: " << childIsGathered
		      << " WILL BE GATHERED: " << childWillBeGathered
		      << " INITIAL RUN: " << tableMan.isInitialRun << std::endl;
	  }
#endif
          if (columns.empty() || childIsGathered || (childWillBeGathered && tableMan.isInitialRun)) {
	    if (childIsGathered || (childWillBeGathered && tableMan.isInitialRun)) {
	      currColumn = childHead;
	      tableMan.currColumn = childHead;
	    }
            auto childArg = lazyGather(lazyChildExpr, tableMan, indicesMan,
                                       columns, url, loader, spanSizeOut,
				       numSpansOut, currColumn, numThreads);
            
	    if (childIsGathered) {
	      args.emplace_back(std::move(childArg));
	      tableMan.markColumnAsLoaded(childHead);
	    }
          }
        } else if (childHead == "DictionaryEncodedList"_) {
	  auto childArg =
	    lazyGatherDictionaryEncodedList(lazyChildExpr, tableMan, indicesMan,
					    columns, url, loader, spanSizeOut, numSpansOut,
					    currColumn, numThreads);
	  
	  bool justCachingDicts = std::find(columns.begin(), columns.end(),
					    currColumn) == columns.end();
	  if (!justCachingDicts) {
	    args.emplace_back(std::move(childArg));
	  }
	} else if (childHead == "BitPackedList"_) {
	  auto childArg =
	    lazyGatherBitPackedList(lazyChildExpr, tableMan, indicesMan,
					    columns, url, loader);
	  args.emplace_back(std::move(childArg));
	} else if (childHead == "SingleValueList"_) {
	  auto childArg =
	    lazyGatherSingleValueList(lazyChildExpr, tableMan, indicesMan,
				      columns, url, loader, spanSizeOut,
				      numSpansOut, numThreads);
	  args.emplace_back(std::move(childArg));
	} else if (childHead == "SequenceEncodedList"_) {
	  auto childArg =
	    lazyGatherSequenceEncodedList(lazyChildExpr, tableMan, indicesMan,
					  columns, url, loader, spanSizeOut,
					  numSpansOut, numThreads);
	  args.emplace_back(std::move(childArg));
	} else if (childHead == "FORList"_) {
	  auto childArg =
	    lazyGatherFrameOfReferenceList(lazyChildExpr, tableMan, indicesMan,
					   columns, url, loader);
	  args.emplace_back(std::move(childArg));
	} else if (childHead == "RLEList"_) {
	  auto childArg =
	    lazyGatherRunLengthEncodedList(lazyChildExpr, tableMan, indicesMan,
					   columns, url, loader);
	  args.emplace_back(std::move(childArg));
	} else {
	  bool currColIsGathered = 
	    std::find(columns.begin(), columns.end(),
		      currColumn) != columns.end();
	  if (!tableMan.isInitialRun || currColIsGathered || (tableMan.isInitialRun && head == "Dictionary"_)) {
	    auto childArg = lazyGather(lazyChildExpr, tableMan, indicesMan,
				       columns, url, loader, spanSizeOut,
				       numSpansOut, currColumn, numThreads);
	    args.emplace_back(std::move(childArg));
	  }
        }
      } else {

        args.emplace_back(std::move(lazyChildExpr.getCurrentExpression()));
      }
    }
  }
  return boss::ComplexExpression(std::move(head), {}, std::move(args),
                                 std::move(spanArgs));
}
  FOR_EACH_INDICES_INT_TYPE(INSTANTIATE_LAZY_GATHER_FOR);


  ExpressionArguments getUniqueURLSFromSpanArgs(const ExpressionSpanArguments &urls) {
    std::unordered_set<std::string> seenURLs;
    ExpressionArguments args;
    std::for_each(
                  urls.begin(), urls.end(),
                  [&args, &seenURLs](const auto &span) {
                    if (std::holds_alternative<boss::Span<std::string const>>(
									      span)) {
                      std::for_each(
				    get<boss::Span<std::string const>>(span).begin(),
				    get<boss::Span<std::string const>>(span).end(),
				    [&](auto url) {
				      if (seenURLs.find(url) == seenURLs.end()) {
					seenURLs.insert(url);
					args.emplace_back(url);
				      }
				    });
                    } else if (std::holds_alternative<boss::Span<std::string>>(
									       span)) {
                      std::for_each(get<boss::Span<std::string>>(span).begin(),
                                    get<boss::Span<std::string>>(span).end(),
                                    [&](auto url) {
                                      if (seenURLs.find(url) ==
                                          seenURLs.end()) {
                                        seenURLs.insert(url);
                                        args.emplace_back(url);
                                      }
                                    });
                    }
                  });
    return std::move(args);
  }

  boss::ComplexExpression Engine::gatherTableNoIndices(const std::string &url, EvalFunction &loader, ExpressionSpanArguments &&indices, const std::vector<boss::Symbol> &columns) {
    auto tableIt = tableMap.find(url);
    if (tableIt == tableMap.end()) {
      tableMap.emplace(url, Engine::TableManager(url, loader));
    }
    auto &tableMan = tableMap[url];

    RootExpression *&root = tableMan.root;
    auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
    auto lazyExpr = expr.lazilyDeserialize();
	      
    IndicesManager<int32_t> indicesMan(std::move(indices), false, -1, -1, duplicateIndicesPossible, "EMPTY INDICES", unsortedIndicesPossible);
    auto table = get<boss::ComplexExpression>(lazyGather(lazyExpr, tableMan, indicesMan, columns,
							 url, loader, -1, -1, "NONE"_, -1));
    return std::move(table);
  }

  boss::Expression Engine::gatherAndMerge(ExpressionArguments &urls, EvalFunction &loader, const std::vector<boss::Symbol> &columns) {
    std::vector<boss::ComplexExpression> tables;
    for (auto i = 0; i < urls.size(); i++) {
      std::string urlStr = std::get<std::string>(urls[i]);
      ExpressionSpanArguments emptyIndices;
      tables.emplace_back(std::move(gatherTableNoIndices(urlStr, loader, std::move(emptyIndices), columns)));
    }
    auto resTable = std::move(tables[0]);
    for (auto it = std::make_move_iterator(std::next(tables.begin()));
	 it != std::make_move_iterator(tables.end()); ++it) {
      resTable =
        std::move(mergeTables(std::move(resTable),
		  std::move(*it)));
    }
    return std::move(resTable);
  }

  bool canRelationalOperatorDuplicateIndices(const boss::Symbol &head) {
    return head == "Join"_;
  }
  
  bool canRelationalOperatorUnsortIndices(const boss::Symbol &head) {
    return head == "Join"_;
  }
  
  bool canRelationalOperatorIntroduceIndices(const boss::Symbol &head) {
    return head == "Union"_;
  }

  bool expressionContainsSymbols(const Expression &e, const std::unordered_set<boss::Symbol> &symbols) {
    return
      std::visit(
         boss::utilities::overload(
	    [&](const ComplexExpression &expr) -> bool {
	      if (symbols.find(expr.getHead()) != symbols.end()) {
		return true;
	      }
	      bool res = false;
	      const auto &dynArgs = expr.getDynamicArguments();
	      for (const auto &arg : dynArgs) {
		res |= expressionContainsSymbols(arg, symbols);
		if (res) {
		  return res;
		}
	      }
	      return res;
	    },
	    [&](const auto &arg) -> bool {
	      return false;
	    }),
	 e);
  }

  bool expressionContainsSelect(const Expression &e) {
    std::unordered_set<boss::Symbol> symbols{"Select"_};
    return expressionContainsSymbols(e, symbols);
  }

  bool expressionContainsJoin(const Expression &e) {
    std::unordered_set<boss::Symbol> symbols{"Join"_};
    return expressionContainsSymbols(e, symbols);
  }

  
  std::tuple<boss::Expression, std::vector<boss::Symbol>, std::vector<boss::Expression>>
  findAllJoinLeaves(ComplexExpression &&expr) {
    std::vector<boss::Symbol> joinPlaceholders;
    std::vector<boss::Expression> subJoinExprs;

    std::function<void(ComplexExpression&)> helper = [&](ComplexExpression &e) -> void {
      auto [head, unused_, dynamics, spans] = std::move(e).decompose();
      for (size_t i = 0; i < dynamics.size(); i++) {
	if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	  auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);
	  if (complexDyn.getHead() == "Join"_) {
	    auto [joinHead, joinUnused_, joinDynamics, joinSpans] = std::move(complexDyn).decompose();

	    bool branchHasJoin1 = expressionContainsJoin(joinDynamics[0]);
	    bool branchHasJoin2 = expressionContainsJoin(joinDynamics[1]);

	    auto& joinBranch1 = std::get<ComplexExpression>(joinDynamics[0]);
	    auto& joinBranch2 = std::get<ComplexExpression>(joinDynamics[1]);

	    if (branchHasJoin1) {
	      helper(joinBranch1);
	    } else {
	      std::string joinStr = "JoinPlaceholder" + std::to_string(joinPlaceholders.size());
	      boss::Symbol joinPlaceholder(joinStr);
	      joinPlaceholders.push_back(joinPlaceholder);
	      subJoinExprs.push_back(std::move(joinBranch1));
	      joinDynamics[0] = joinPlaceholder;
	    }
	    
	    if (branchHasJoin2) {
	      helper(joinBranch2);
	    } else {
	      std::string joinStr = "JoinPlaceholder" + std::to_string(joinPlaceholders.size());
	      boss::Symbol joinPlaceholder(joinStr);
	      joinPlaceholders.push_back(joinPlaceholder);
	      subJoinExprs.push_back(std::move(joinBranch2));
	      joinDynamics[1] = joinPlaceholder;
	    }
	    auto reconstructedJoin = ComplexExpression(std::move(joinHead), {}, std::move(joinDynamics), std::move(joinSpans));
	    dynamics[i] = std::move(reconstructedJoin);
	  } else {
	    helper(complexDyn);
	  }
	}
      }
      e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    };

    helper(expr);
    if (joinPlaceholders.size() != subJoinExprs.size()) {
      throw std::runtime_error("Inconsistent join leaves extraction.");
    }
    return {std::move(expr), std::move(joinPlaceholders), std::move(subJoinExprs)};
  }
  
  boss::Expression inputAllPlaceholdersIntoExpression(boss::Expression &&expr, const std::vector<boss::Symbol> &tablePlaceholders, std::vector<boss::Expression> &&tableExprs) {
    if (!std::holds_alternative<ComplexExpression>(expr)) {
      return std::move(expr);
    }
    
    std::function<void(ComplexExpression&)> helper = [&](ComplexExpression &e) -> void {
      auto [head, unused_, dynamics, spans] = std::move(e).decompose();
      for (size_t i = 0; i < dynamics.size(); i++) {
	if (std::holds_alternative<boss::Symbol>(dynamics[i])) {
	  const auto it = std::find(tablePlaceholders.begin(),
				    tablePlaceholders.end(),
				    std::get<boss::Symbol>(dynamics[i]));
	  if (it != tablePlaceholders.end()) {
	    size_t index = std::distance(tablePlaceholders.begin(), it);
	    dynamics[i] = std::move(tableExprs[index]);
	  }
	} else if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	  auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);
	  helper(complexDyn);
	}
      }

      e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    };

    auto& complexDyn = std::get<ComplexExpression>(expr);
    helper(complexDyn);
    return std::move(expr);
  }

  std::unordered_set<boss::Symbol> getAllSymbolsFromExpression(const boss::Expression &expr) {
    std::unordered_set<boss::Symbol> allSymbols;
    if (std::holds_alternative<boss::Symbol>(expr)) {
      allSymbols.insert(std::get<boss::Symbol>(expr));
    }
    if (!std::holds_alternative<ComplexExpression>(expr)) {
      return std::move(allSymbols);
    }
    
    std::function<void(const ComplexExpression&)> helper = [&](const ComplexExpression &e) -> void {
      const auto& dynamics = e.getDynamicArguments();
      for (size_t i = 0; i < dynamics.size(); i++) {
	if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	  const auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);
	  helper(complexDyn);
	} else if (std::holds_alternative<boss::Symbol>(dynamics[i])) {
	  allSymbols.insert(std::get<boss::Symbol>(dynamics[i]));
	}
      }
    };
    const auto& complexExpr = std::get<ComplexExpression>(expr);
    helper(complexExpr);

    return std::move(allSymbols);
  }

  boss::Expression
  Engine::orderSelectionsByRemoteSelectivity(ComplexExpression &&expr,
					     std::unordered_map<std::string, TableManager> &tableMap) {
    auto [outerJoinsExpr, joinPlaceholders, subJoinExprs] = findAllJoinLeaves(std::move(expr));

    std::function<std::string(ComplexExpression&)> findUrl = [&](ComplexExpression &e) -> std::string {
      std::string url = "EMPTY";
      auto [head, unused_, dynamics, spans] = std::move(e).decompose();
      for (size_t i = 0; i < dynamics.size(); i++) {
	if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	  auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);

	  if (complexDyn.getHead() == "Gather"_) {
	    auto [gatherHead, gatherUnused_, gatherDynamics, gatherSpans] = std::move(complexDyn).decompose();
	    url = std::get<std::string>(gatherDynamics[0]);
	    auto reconstructedGather = ComplexExpression(std::move(gatherHead), {}, std::move(gatherDynamics), std::move(gatherSpans));
	    dynamics[i] = std::move(reconstructedGather);
	  } else {
	    url = findUrl(complexDyn);
	  }
	}
      }
      e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
      return url;
    };
    
    boss::Symbol possiblePlaceholder = "PossibleSelection"_;
    boss::Symbol ignore = "Ignore"_;
    for (size_t i = 0; i < subJoinExprs.size(); i++) {
      auto& joinLeaf = std::get<ComplexExpression>(subJoinExprs[i]);
      // Find url
      std::string url = findUrl(joinLeaf);
      if (tableMap.find(url) == tableMap.end()) {
	continue;
      }
      auto& tableMan = tableMap[url];

      // Find all selections
      std::vector<boss::Expression> subExprs;
      std::vector<boss::Expression> whereExprs;

      std::function<void(ComplexExpression&)> helper = [&](ComplexExpression &e) -> void {
	auto [head, unused_, dynamics, spans] = std::move(e).decompose();
	for (size_t i = 0; i < dynamics.size(); i++) {
	  if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	    auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);

	    if (complexDyn.getHead() == "Select"_) {
	      auto [selectHead, selectUnused_, selectDynamics, selectSpans] = std::move(complexDyn).decompose();
	      bool exprHasSelect = expressionContainsSelect(selectDynamics[0]);

	      auto& subExpr = std::get<ComplexExpression>(selectDynamics[0]);
	      auto& whereExpr = std::get<ComplexExpression>(selectDynamics[1]);
	      
	      if (exprHasSelect) {
		helper(subExpr);
	      } else {
		subExprs.push_back(std::move(subExpr));
		whereExprs.push_back(std::move(whereExpr));
		selectDynamics[0] = possiblePlaceholder;
		selectDynamics[1] = ignore;
	      }
	      auto reconstructedSelect = ComplexExpression(std::move(selectHead), {}, std::move(selectDynamics), std::move(selectSpans));
	      dynamics[i] = std::move(reconstructedSelect);
	    } else {
	      helper(complexDyn);
	    }
	  }
	}
	e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
      };
      helper(joinLeaf);

      // Extract columns from wheres
      std::vector<std::unordered_set<boss::Symbol>> selectColumns;
      for (size_t i = 0; i < whereExprs.size(); i++) {
	selectColumns.push_back(std::move(getAllSymbolsFromExpression(whereExprs[i])));
      }
      
      // Map columns to locality score -- higher is worse
      std::vector<int64_t> selectScores;
      for (const auto& cols : selectColumns) {
	int64_t score = 0;
	for (const auto& sym : cols) {
	  if (tableMan.columnBitsets.find(sym) == tableMan.columnBitsets.end()) {
	    score += tableMan.numRows;
	  } else {
	    auto& bitset = tableMan.columnBitsets[sym];
	    score += (tableMan.numRows - bitset.numSetValues());
	  }
	}
	selectScores.push_back(score);
      }

      std::vector<size_t> idx(selectScores.size());
      std::iota(idx.begin(), idx.end(), 0);
      std::stable_sort(idx.begin(), idx.end(),
		       [&selectScores](size_t i1, size_t i2) {
			 return selectScores[i1] > selectScores[i2];
		       });

      // insert new order
      std::function<void(ComplexExpression&, size_t)> insertionHelper = [&](ComplexExpression &e, size_t selectI) -> void {
	auto [head, unused_, dynamics, spans] = std::move(e).decompose();
	for (size_t i = 0; i < dynamics.size(); i++) {
	  if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	    auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);

	    if (complexDyn.getHead() == "Select"_) {
	      auto [selectHead, selectUnused_, selectDynamics, selectSpans] = std::move(complexDyn).decompose();

	      if (std::holds_alternative<boss::Symbol>(selectDynamics[0]) &&
		  std::get<boss::Symbol>(selectDynamics[0]) == possiblePlaceholder) {
		selectDynamics[0] = std::move(subExprs[selectI]);
		selectDynamics[1] = std::move(whereExprs[selectI]);
	      } else {
		auto& subExpr = std::get<ComplexExpression>(selectDynamics[0]);
		insertionHelper(subExpr, selectI);
	      }
	      auto reconstructedSelect = ComplexExpression(std::move(selectHead), {}, std::move(selectDynamics), std::move(selectSpans));
	      dynamics[i] = std::move(reconstructedSelect);
	    } else {
	      helper(complexDyn);
	    }
	  }
	}
	e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
      };

      for (size_t i : idx) {
	insertionHelper(joinLeaf, i);
      }
    }

    return inputAllPlaceholdersIntoExpression(std::move(outerJoinsExpr),
							joinPlaceholders,
							std::move(subJoinExprs));
  }

  boss::Expression Engine::evaluate(Expression &&e, const std::unordered_set<boss::Symbol> &allSymbols) {
  return std::visit(
      boss::utilities::overload(
	 [this, &allSymbols](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "Parse"_) {
              if (dynamics.empty()) {
                throw std::runtime_error("Wisent Deserialiser: No "
                                         "subexpressions in Parse_ expression");
              }

              if (std::holds_alternative<boss::ComplexExpression>(
                      dynamics[0])) {
                auto &subExpr = std::get<boss::ComplexExpression>(dynamics[0]);
                if (subExpr.getHead() == "ByteSequence"_) {
                  auto res = deserialiseByteSequenceMove(std::move(subExpr));
                  return std::move(res);
                }
              }
            } else if (head == "GetEngineCapabilities"_) {
              return std::move("List"_("Parse"_, "ParseTables"_, "Gather"_,
                                       "GatherRanges"_, "GatherTables"_,
				       "ClearWisentCaches"_, "SetNumSpansOut"_,
				       "SetNumThreads"_, "GetEncodingForFromURL"_));
            } else if (head == "ClearWisentCaches"_) {
	      tableMap.clear();
	      return "CachesCleared"_;
            } else if (head == "GetEncodingForFromURL"_) {
	      if (dynamics.size() != 3) {
		throw std::runtime_error("Wisent Deserialiser: Invalid number of arguments in GetEncodingForFromURL_ expression");
	      }
	      auto& url = std::get<std::string>(dynamics[0]);
	      auto& str = std::get<std::string>(dynamics[1]);
	      auto& col = std::get<boss::Symbol>(dynamics[2]);

	      auto tableIt = tableMap.find(url);
	      if (tableIt == tableMap.end()) {
		throw std::runtime_error("Wisent Deserialiser: Invalid URL in GetEncodingForFromURL_");	
	      }
	      auto& tableMan = tableMap[url];

	      auto dictIt = tableMan.dictionaryMap.find(col);
	      if (dictIt == tableMan.dictionaryMap.end()) {
		throw std::runtime_error("Wisent Deserialiser: No dict for col in GetEncodingForFromURL_");	
	      }
	      auto& dictExpr = tableMan.dictionaryMap[col];
	      auto& dictDynamics = std::get<boss::ComplexExpression>(dictExpr).getDynamicArguments();
	      auto& dictListExpr = dictDynamics[0];
	      auto& dlSpans = std::get<boss::ComplexExpression>(dictListExpr).getSpanArguments();
	      const auto& dictSpan = dlSpans[0];
	      int64_t encoding = -1;
	      std::visit([&](const auto& typedDict) {
		const size_t numVals = typedDict.size();
		const auto& val0 = typedDict[0];
		using ArgType = std::decay_t<decltype(val0)>;
		if constexpr(std::is_same_v<ArgType, std::string>) {
		  for (size_t i = 0; i < numVals; i++) {
		    if (typedDict[i] == str) {
		      encoding = i;
		    }
		  }
		}
	      }, dictSpan);

	      if (encoding < 0) {
		throw std::runtime_error("Wisent Deserialiser: Could not find encoding for str in GetEncodingForFromURL_");
	      }
	      
	      return encoding;
            } else if (head == "SetNumSpansOut"_) {
	      if (dynamics.size() != 1) {
		throw std::runtime_error("Wisent Deserialiser: Invalid number of arguments in SetNumSpansOut_ expression");
	      }
	      if (!std::holds_alternative<int64_t>(dynamics[0])) {
		throw std::runtime_error("Wisent Deserialiser: Argument for SetNumSpansOut_ expression must be int64_t");
	      }
	      paramNumSpansOut = std::get<int64_t>(dynamics[0]);
	      return "NumSpansOutSetTo"_(paramNumSpansOut);
            } else if (head == "SetNumThreads"_) {
	      if (dynamics.size() != 1) {
		throw std::runtime_error("Wisent Deserialiser: Invalid number of arguments in SetNumThreads_ expression");
	      }
	      if (!std::holds_alternative<int64_t>(dynamics[0])) {
		throw std::runtime_error("Wisent Deserialiser: Argument for SetNumThreads_ expression must be int64_t");
	      }
	      paramNumThreads = std::get<int64_t>(dynamics[0]);
	      return "NumThreadsSetTo"_(paramNumThreads);
            } else if (head == "ParseTables"_) {
              if (dynamics.empty()) {
                throw std::runtime_error("Wisent Deserialiser: No "
                                         " urls in ParseTables_ expression");
              }
              const auto &loaderPath = get<std::string>(dynamics[0]);
              EvalFunction loader = reinterpret_cast<EvalFunction>(
                  libraries.at(loaderPath).evaluateFunction);
              ExpressionSpanArguments urls;
              if (dynamics.size() == 3) {
                auto colHead = std::move(get<boss::Symbol>(dynamics[1]));
                urls = getExpressionSpanArgs(
                    std::move(get<boss::ComplexExpression>(dynamics[2])),
                    std::move(colHead));
              } else if (dynamics.size() == 2) {
                urls = getExpressionSpanArgs(
                    std::move(get<boss::ComplexExpression>(dynamics[1])));
              }
              ExpressionArguments args = getUniqueURLSFromSpanArgs(urls);
              auto fetchExpr =
                  boss::ComplexExpression("Fetch"_, {}, std::move(args), {});
	      auto byteSeqs = applyEngine(std::move(fetchExpr), loader);

              return parseByteSequences(
                  std::move(get<boss::ComplexExpression>(byteSeqs)));
            } else if (head == "Gather"_) {
              if (dynamics.size() < 4 || dynamics.size() > 8) {
                throw std::runtime_error(
                    "Wisent Deserialiser: Gather_ expression must be of form "
                    "Gather_[url, loaderPath, List_[List_[spanIndices...]]], "
                    "List_[column...], maxRanges?, indicesColumnName?, numThreads?, numSpansOut?]");
              }
#ifdef DEBUG
	      std::cout << "##################### WISENT ENGINE #######################" << std::endl;
#endif
	      const auto dynSize = dynamics.size();
              const auto &url = get<std::string>(dynamics[0]);
              const auto &loaderPath = get<std::string>(dynamics[1]);
              EvalFunction loader = reinterpret_cast<EvalFunction>(
                  libraries.at(loaderPath).evaluateFunction);
              auto indices = getExpressionSpanArgs(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[2]))));
	      auto columns = getColumns(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[3]))));

              maxRanges = DEFAULT_MAX_RANGES;
              if (dynSize > 4) {
		CHANGEABLE_RANGES = get<int64_t>(dynamics[4]);
		maxRanges = -1;
              }

	      boss::Symbol indicesColumnName = "__internal_indices_"_;
	      if (dynSize > 5) {
	        indicesColumnName = std::move(get<boss::Symbol>(dynamics[5]));
	      }

	      if (dynSize > 6) {
		NUM_THREADS = get<int64_t>(dynamics[6]);
	      }

	      size_t localSpansOut = paramNumSpansOut;
	      if (dynSize > 7) {
		localSpansOut = get<int64_t>(dynamics[7]);
	      }

	      auto tableIt = tableMap.find(url);
	      if (tableIt == tableMap.end()) {
	        tableMap.emplace(url, Engine::TableManager(url, loader, allSymbols));
	      }
	      auto &tableMan = tableMap[url];

	      if (tableMan.numRows >= 0) {
	        tableMan.initColumnBitsets(columns, paramValsPerBit);
	      }

	      RootExpression *&root = tableMan.root;
	      auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
	      auto lazyExpr = expr.lazilyDeserialize();
	      
	      IndicesManager<int32_t> indicesMan(std::move(indices), false, tableMan.numRows, paramValsPerBit, duplicateIndicesPossible, "INITIAL INDICES", unsortedIndicesPossible);
	      
	      constexpr int64_t SPAN_SIZE = -1;
	      boss::ComplexExpression table = "EmptyTable"_();
	      table = get<boss::ComplexExpression>(lazyGather(lazyExpr, tableMan, indicesMan, columns,
							      url, loader, SPAN_SIZE, localSpansOut, "NONE"_, paramNumThreads < 0 ? 20 : paramNumThreads));
	      if (indicesMan.indices.empty()) {
		auto res = addIndexColumnToTable(std::move(table), std::move(indicesColumnName), 3);
		if (tableMan.numRows < 0) {
		  size_t rows = 0;
		  const auto& col0 = std::get<boss::ComplexExpression>(std::get<boss::ComplexExpression>(res).getDynamicArguments()[0]);
		  const auto& list0 = std::get<boss::ComplexExpression>(col0.getDynamicArguments()[0]);
		  const auto& listSpans = list0.getSpanArguments();
		  for (auto const& listSpan : list0.getSpanArguments()) {
		    std::visit([&](auto const& typedListSpan) {
		      rows += typedListSpan.size();
		    }, listSpan);
		  }
		  tableMan.numRows = rows;
		}
		tableMan.initColumnBitsets(columns, paramValsPerBit, true);
		if (tableMan.isInitialRun) {
		  tableMan.isInitialRun = false;
		}
		return std::move(res);
	      }
	      auto idxArgs = indicesMan.getIndicesAsExpressionSpanArguments();
	      boss::Symbol indicesColNameCopy = indicesColumnName;
	      auto res = addIndexColumnToTable(std::move(table), std::move(idxArgs), std::move(indicesColumnName));
	      return std::move(res);
            } else if (head == "GatherTables"_) {
              if (dynamics.size() < 5 || dynamics.size() > 8) {
                throw std::runtime_error(
                    "Wisent Deserialiser: Gather_ expression must be of form "
                    "GatherTables_[loaderPath, urlColumn, urlTable, List_[List_[spanIndices...]]], "
                    "List_[column...], maxRanges?, indicesColumnName?, numThreads?]");
              }
              const auto &loaderPath = get<std::string>(dynamics[0]);
              auto &urlColumn = get<boss::Symbol>(dynamics[1]);
              EvalFunction loader = reinterpret_cast<EvalFunction>(
                  libraries.at(loaderPath).evaluateFunction);

	      ExpressionSpanArguments urls =
		getExpressionSpanArgs(std::move(get<boss::ComplexExpression>(dynamics[2])),
				      std::move(urlColumn));
              ExpressionArguments urlArgs = getUniqueURLSFromSpanArgs(urls);
              
	      auto indices = getExpressionSpanArgs(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[3]))));
              auto columns = getColumns(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[4]))));

              maxRanges = DEFAULT_MAX_RANGES;
              if (dynamics.size() > 5) {
		CHANGEABLE_RANGES = get<int64_t>(dynamics[4]);
		maxRanges = -1;
              }

	      boss::Symbol indicesColumnName = "__internal_indices_"_;
	      if (dynamics.size() > 5) {
	        indicesColumnName = std::move(get<boss::Symbol>(dynamics[5]));
	      }

	      if (dynamics.size() > 6) {
		NUM_THREADS = get<int64_t>(dynamics[6]);
	      }

	      return std::move(gatherAndMerge(urlArgs, loader, columns));
            } else if (head == "GatherRanges"_) {
              if (dynamics.size() < 6 || dynamics.size() > 7) {
                throw std::runtime_error(
                    "Wisent Deserialiser: Gather_ expression must be of form "
                    "GatherRanges_[url, loaderPath, startColName, endColName, "
                    "Table_[col[List_[spanIndices...]]]], "
                    "List_[column...], maxRanges?]");
              }
              const auto &url = get<std::string>(dynamics[0]);
              const auto &loaderPath = get<std::string>(dynamics[1]);
              auto startColName = std::move(get<boss::Symbol>(dynamics[2]));
              auto endColName = std::move(get<boss::Symbol>(dynamics[3]));
              EvalFunction loader = reinterpret_cast<EvalFunction>(
                  libraries.at(loaderPath).evaluateFunction);
              auto indices = getExpressionSpanArgsRanged(
                  std::move(
                      get<boss::ComplexExpression>(std::move(dynamics[4]))),
                  std::move(startColName), std::move(endColName));
              auto columns = getColumns(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[5]))));

              maxRanges = DEFAULT_MAX_RANGES;
              if (dynamics.size() > 6) {
                maxRanges = get<int64_t>(dynamics[6]);
                maxRanges = maxRanges < 1 ? DEFAULT_MAX_RANGES : maxRanges;
              }
	      
	      auto tableIt = tableMap.find(url);
	      if (tableIt == tableMap.end()) {
		tableMap.emplace(url, Engine::TableManager(url, loader));
	      }
	      auto &tableMan = tableMap[url];
	      
	      RootExpression *&root = tableMan.root;
	      
	      auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
              auto lazyExpr = expr.lazilyDeserialize();
              
	      IndicesManager<int32_t> indicesMan(std::move(indices), true, tableMan.numRows, paramValsPerBit, duplicateIndicesPossible, "INITIAL GATHER RANGES", unsortedIndicesPossible);
	      constexpr int64_t SPAN_SIZE = -1;
              auto res = lazyGather(lazyExpr, tableMan, indicesMan, columns,
                                    url, loader, SPAN_SIZE, -1, "NONE"_, -1);
              return std::move(res);
            }

	    if (!duplicateIndicesPossible) {
	      duplicateIndicesPossible = canRelationalOperatorDuplicateIndices(head);
	    }
	    
	    if (!unsortedIndicesPossible) {
	      unsortedIndicesPossible = canRelationalOperatorUnsortIndices(head);
	    }
	    
	    if (!newIndicesPossible) {
	      newIndicesPossible = canRelationalOperatorIntroduceIndices(head);
	    }
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this, &allSymbols](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg), allSymbols);
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

  boss::Expression Engine::evaluate(boss::Expression &&e) {
    const auto allSymbols = getAllSymbolsFromExpression(e);
    if (std::holds_alternative<ComplexExpression>(e)) {
      e = std::move(orderSelectionsByRemoteSelectivity(std::move(std::get<ComplexExpression>(e)), tableMap));
    }
    return evaluate(std::move(e), allSymbols);
  }
} // namespace boss::engines::WisentDeserialiser

static auto &enginePtr(bool initialise = true) {
  static std::mutex m;
  std::lock_guard const lock(m);
  static auto engine =
      std::unique_ptr<boss::engines::WisentDeserialiser::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::WisentDeserialiser::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
