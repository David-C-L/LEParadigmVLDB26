#pragma once

#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include <memory>

using namespace facebook::velox;

namespace boss::engines::velox {

// Factory function to create compacted vector based on type
VectorPtr createCompactedVectorFromNulls(memory::MemoryPool* pool, const VectorPtr& sourceVector);

} // namespace boss::engines::velox
