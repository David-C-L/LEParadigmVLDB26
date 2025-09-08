#pragma once
#include <vector>
#include <queue>
#include <cmath>
#include <algorithm>
#include <cstdint>
#include <numeric>

struct alignas(8) Range {
  int32_t start;
  int32_t end;

  constexpr Range() noexcept = default;
  constexpr Range(int32_t s, int32_t e) noexcept : start(s), end(e) {}

  constexpr size_t size() const noexcept { return end - start; }
  constexpr int32_t actualValues() const noexcept { return end - start; }
  constexpr bool empty() const noexcept { return start >= end; }
  constexpr bool contains(size_t value) const noexcept { 
    return value >= start && value < end; 
  }

  constexpr bool canMerge(const Range& other, size_t gap = 0) const noexcept {
    return other.start <= end + gap;
  }
    
  constexpr Range merge(const Range& other) const noexcept {
    return {std::min(start, other.start), std::max(end, other.end)};
  }
    
  constexpr bool overlaps(const Range& other) const noexcept {
    return start < other.end && other.start < end;
  }

  constexpr int32_t effectiveCost() const { return size() + 4; }

  constexpr int64_t gapTo(const Range& other) const { 
    return other.start - end; 
  }
};
    
struct EfficiencyMetrics {
  double efficiency;
  int32_t totalCost;
  int32_t usefulValues;
  int32_t wastedValues;
  int32_t overheadCost;
};
    
EfficiencyMetrics calculateMetrics(const std::vector<Range>& ranges,
				   int32_t actualIndicesCount) {
  EfficiencyMetrics metrics = {};
        
  for (const auto& range : ranges) {
    metrics.totalCost += range.effectiveCost();
  }
        
  metrics.usefulValues = actualIndicesCount;
  metrics.wastedValues = std::accumulate(ranges.begin(), ranges.end(), 0,
					  [](int32_t sum, const Range& r) { return sum + r.size(); }) - actualIndicesCount;
  metrics.overheadCost = ranges.size() * 4;
  metrics.efficiency = static_cast<double>(metrics.usefulValues) / metrics.totalCost;
        
  return metrics;
}
