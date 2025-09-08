#pragma once

#define FOR_EACH_INDICES_INT_TYPE(FUNC_MACRO) \
  FUNC_MACRO(int32_t) \
  FUNC_MACRO(int64_t)

#define INSTANTIATE_DECOMPRESS_FOR_FOR(T) template boss::Expression Engine::decompressFOR<T>(boss::ComplexExpression&&, IndicesManager<T>&, boss::ComplexExpression&&, IndicesManager<T>&, int32_t);

#define INSTANTIATE_GET_SPAN_FROM_INDICES_FOR(T) template boss::expressions::ExpressionSpanArguments Engine::getSpanFromIndices<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::string&, EvalFunction&, bool, int64_t, int64_t, int64_t, boss::Symbol);

#define INSTANTIATE_GET_SPAN_FROM_INDEX_RANGES_FOR(T) template boss::expressions::ExpressionSpanArgument Engine::getSpanFromIndexRanges<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::string&, EvalFunction&, bool);

#define INSTANTIATE_DERIVE_RLE_VALUE_INDICES_FROM_RLE_STARTS_FOR(T) template ExpressionSpanArguments Engine::deriveRLEValueIndicesFromRLEStarts<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::string&, EvalFunction&);

#define INSTANTIATE_LAZY_GATHER_FOR(T) template boss::Expression Engine::lazyGather<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::vector<boss::Symbol>&, const std::string&, EvalFunction&, int64_t, int64_t, boss::Symbol, int64_t);

#define INSTANTIATE_LAZY_GATHER_DICTIONARY_ENCODED_LIST_FOR(T) template boss::Expression Engine::lazyGatherDictionaryEncodedList<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::vector<boss::Symbol>&, const std::string&, EvalFunction&, int64_t, int64_t, boss::Symbol, int64_t);

#define INSTANTIATE_LAZY_GATHER_BIT_PACKED_LIST_FOR(T) template boss::Expression Engine::lazyGatherBitPackedList<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::vector<boss::Symbol>&, const std::string&, EvalFunction&);

#define INSTANTIATE_LAZY_GATHER_SINGLE_VALUE_LIST_FOR(T) template boss::Expression Engine::lazyGatherSingleValueList<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::vector<boss::Symbol>&, const std::string&, EvalFunction&, int64_t, int64_t, int64_t);

#define INSTANTIATE_LAZY_GATHER_SEQUENCE_ENCODED_LIST_FOR(T) template boss::Expression Engine::lazyGatherSequenceEncodedList<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::vector<boss::Symbol>&, const std::string&, EvalFunction&, int64_t, int64_t, int64_t);

#define INSTANTIATE_LAZY_GATHER_FRAME_OF_REFERENCE_LIST_FOR(T) template boss::Expression Engine::lazyGatherFrameOfReferenceList<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::vector<boss::Symbol>&, const std::string&, EvalFunction&);

#define INSTANTIATE_LAZY_GATHER_RUN_LENGTH_ENCODED_LIST_FOR(T) template boss::Expression Engine::lazyGatherRunLengthEncodedList<T>(LazilyDeserializedExpression&, TableManager&, IndicesManager<T>&, const std::vector<boss::Symbol>&, const std::string&, EvalFunction&);
