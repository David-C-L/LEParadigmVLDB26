#include "CompactNullVector.h"

using namespace facebook::velox;

namespace boss::engines::velox {

// Use the same createFlatVector template function as in BridgeVelox.cpp
template <TypeKind kind>
VectorPtr createCompactedFlatVector(memory::MemoryPool* pool, TypePtr const& type, 
                                   BufferPtr nulls, size_t length, BufferPtr values) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<FlatVector<T>>(pool, type, nulls, length, values,
                                         std::vector<BufferPtr>(), SimpleVectorStats<T>{},
                                         std::nullopt, std::nullopt);
}

template<typename T>
VectorPtr createCompactedVector(memory::MemoryPool* pool, const VectorPtr& sourceVector) {
  if (!sourceVector->nulls()) {
    // No nulls, return as-is
    return sourceVector;
  }
  
  // Count non-null elements and collect their values
  std::vector<T> validValues;
  validValues.reserve(sourceVector->size());
  
  auto* flatVector = sourceVector->as<FlatVector<T>>();
  for (vector_size_t i = 0; i < sourceVector->size(); ++i) {
    if (!sourceVector->isNullAt(i)) {
      validValues.push_back(flatVector->valueAt(i));
    }
  }
  
  if (validValues.empty()) {
    // Return empty vector of correct type using the same pattern
    auto type = CppToType<T>::create();
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(createCompactedFlatVector, type->kind(), 
                                             pool, type, nullptr, 0, nullptr);
  }
  
  // Create buffer for compacted values
  auto numElements = validValues.size();
  auto dataBuffer = AlignedBuffer::allocate<T>(numElements, pool);
  auto* data = dataBuffer->template asMutable<T>();
  
  // Copy valid values
  std::memcpy(data, validValues.data(), numElements * sizeof(T));
  
  // Create compacted FlatVector using the same pattern as your existing code
  auto type = CppToType<T>::create();
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(createCompactedFlatVector, type->kind(), 
                                           pool, type, nullptr, numElements, dataBuffer);
}

VectorPtr createCompactedVectorFromNulls(memory::MemoryPool* pool, const VectorPtr& sourceVector) {
  if (!sourceVector->nulls()) {
    return sourceVector;
  }
  
  switch (sourceVector->typeKind()) {
    case TypeKind::TINYINT:
      return createCompactedVector<int8_t>(pool, sourceVector);
    case TypeKind::SMALLINT:
      return createCompactedVector<int16_t>(pool, sourceVector);
    case TypeKind::INTEGER:
      return createCompactedVector<int32_t>(pool, sourceVector);
    case TypeKind::BIGINT:
      return createCompactedVector<int64_t>(pool, sourceVector);
    case TypeKind::REAL:
      return createCompactedVector<float>(pool, sourceVector);
    case TypeKind::DOUBLE:
      return createCompactedVector<double>(pool, sourceVector);
    default:
      throw std::runtime_error("Unsupported type for compacting: " + 
                               facebook::velox::mapTypeKindToName(sourceVector->typeKind()));
  }
}

} // namespace boss::engines::velox
