#pragma once
#ifndef WISENT_DESERIALISER_H
#define WISENT_DESERIALISER_H

#include "RangeOptimiser.hpp"

#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Serialization.hpp>
#include <PortableBOSSSerialization.h>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <robin_hood.h>
#include <boost/dynamic_bitset.hpp>
#include <concepts>
#ifndef _WIN32
#include <dlfcn.h>
#else
#include <omp.h>
#include <execution>
#include <filesystem>
#include <iostream>
#define NOMINMAX // max macro in minwindef.h interfering with std::max...
#include <windows.h>
constexpr static int RTLD_NOW = 0;
constexpr static int RTLD_NODELETE = 0;

// #define HEADERDEBUG

static void *dlopen(LPCSTR lpLibFileName, int /*flags*/) {
  void *libraryPtr = LoadLibrary(lpLibFileName);
  if (libraryPtr != nullptr) {
    return libraryPtr;
  }
  // if it failed to load the standard way (searching dependent dlls in the exe
  // path) try one more time, with loading the dependent dlls from the dll path
  auto filepath = ::std::filesystem::path(lpLibFileName);
  if (filepath.is_absolute()) {
    return LoadLibraryEx(lpLibFileName, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
  } else {
    auto absFilepath = ::std::filesystem::absolute(filepath).string();
    LPCSTR lpAbsFileName = absFilepath.c_str();
    return LoadLibraryEx(lpAbsFileName, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
  }
}
static auto dlclose(void *hModule) {
  auto resetFunction = GetProcAddress((HMODULE)hModule, "reset");
  if (resetFunction != NULL) {
    (*reinterpret_cast<void (*)()>(resetFunction))();
  }
  return FreeLibrary((HMODULE)hModule);
}
static auto dlerror() {
  auto errorCode = GetLastError();
  LPSTR pBuffer = NULL;
  auto msg =
      FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS |
                        FORMAT_MESSAGE_ALLOCATE_BUFFER,
                    NULL, errorCode, 0, (LPSTR)&pBuffer, 0, NULL);
  if (msg > 0) {
    // Assign buffer to smart pointer with custom deleter so that memory gets
    // released in case String's constructor throws an exception.
    auto deleter = [](void *p) { ::LocalFree(p); };
    ::std::unique_ptr<TCHAR, decltype(deleter)> ptrBuffer(pBuffer, deleter);
    return "(" + ::std::to_string(errorCode) + ") " +
           ::std::string(ptrBuffer.get(), msg);
  }
  return ::std::to_string(errorCode);
}
static void *dlsym(void *hModule, LPCSTR lpProcName) {
  return GetProcAddress((HMODULE)hModule, lpProcName);
}
#endif // _WIN32

template<typename T>
concept IndicesInt = std::same_as<T, int32_t> || std::same_as<T, int64_t>;

constexpr uint32_t log2_exact(size_t val) {
  assert(val != 0 && (val & (val - 1)) == 0);
  uint32_t result = 0;
  while (val >>= 1) ++result;
  return result;
}

constexpr static int64_t FETCH_PADDING = 0;
constexpr static int64_t FETCH_ALIGNMENT = 4096;
constexpr static int64_t DEFAULT_MAX_RANGES = 512;
static inline const int64_t ABSOLUTE_MAX_RANGES = 1024*1024;
static inline const int64_t ABSOLUTE_MAX_REQUESTS = 1;

extern int64_t NUM_THREADS;
int64_t NUM_THREADS = 6;

extern int64_t CHANGEABLE_RANGES;
int64_t CHANGEABLE_RANGES = ABSOLUTE_MAX_RANGES;

constexpr static int64_t THREADING_THRESHOLD = 2560000000;
constexpr static int64_t SIMD_THRESHOLD = THREADING_THRESHOLD;

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::ExpressionArguments;
using boss::Span;
using boss::Symbol;
using boss::expressions::ExpressionSpanArguments;
using boss::Expression;
using boss::serialization::Argument;
using boss::serialization::ArgumentType;
using boss::serialization::RootExpression;
using boss::serialization::SerializedExpression;
using LazilyDeserializedExpression = boss::serialization::SerializedExpression<
    nullptr, nullptr, nullptr>::LazilyDeserializedExpression;
using SerializationExpression = boss::serialization::Expression;


using EvalFunction = BOSSExpression *(*)(BOSSExpression *);

template <typename T>
std::vector<std::pair<T, T>> createEvenIntervals(const std::vector<T>& sortedValues, int32_t numRanges);

template <typename T>
std::vector<std::pair<T, T>> createMinimisedIntervals(const std::vector<T>& sortedValues, int32_t numRanges);

template <typename T>
std::vector<std::pair<T, T>> processIntervalsAsync(const std::vector<T>& sortedList, int32_t numRanges, int32_t numPartitions);

template <typename T>
std::pair<std::vector<std::pair<T, T>>, std::vector<T>>
createAdjacentIntervalsAndNonAdjacents(const std::vector<T>& sortedValues);

boss::Expression
createFetchExpression(const std::string &url,
		      std::vector<int64_t> &bounds, bool trackingCache, int64_t numArgs = -1);

boss::Expression applyEngine(Expression &&e, EvalFunction eval);

boss::Span<int8_t> getByteSequence(boss::ComplexExpression &&expression);
namespace boss::engines::WisentDeserialiser {

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

  boss::Expression evaluate(boss::Expression &&e, const std::unordered_set<boss::Symbol> &allSymbols);
  boss::Expression evaluate(boss::Expression &&e);

  struct LibraryAndEvaluateFunction {
    void *library, *evaluateFunction;
  };

  struct LibraryCache
      : private ::std::unordered_map<::std::string,
                                     LibraryAndEvaluateFunction> {
    LibraryAndEvaluateFunction const &at(::std::string const &libraryPath) {
      if (count(libraryPath) == 0) {
        const auto *n = libraryPath.c_str();
        if (auto *library = dlopen(
                n, RTLD_NOW | RTLD_NODELETE)) { // NOLINT(hicpp-signed-bitwise)
          if (auto *sym = dlsym(library, "evaluate")) {
            emplace(libraryPath, LibraryAndEvaluateFunction{library, sym});
          } else {
            throw ::std::runtime_error(
                "library \"" + libraryPath +
                "\" does not provide an evaluate function: " + dlerror());
          }
        } else {
          throw ::std::runtime_error("library \"" + libraryPath +
                                     "\" could not be loaded: " + dlerror());
        }
      };
      return unordered_map::at(libraryPath);
    }
    ~LibraryCache() {
      for (const auto &[name, library] : *this) {
        dlclose(library.library);
      }
    }

    LibraryCache() = default;
    LibraryCache(LibraryCache const &) = delete;
    LibraryCache(LibraryCache &&) = default;
    LibraryCache &operator=(LibraryCache const &) = delete;
    LibraryCache &operator=(LibraryCache &&) = delete;
  } libraries;

private:


  struct FastBitset {
    std::vector<uint64_t> bits_;
    size_t valuesPerBit_;
    size_t numBits_;
    uint32_t shift_;

    explicit FastBitset(size_t numValues, size_t valuesPerBit)
      : valuesPerBit_(valuesPerBit),
	shift_(log2_exact(valuesPerBit)) {
      
      if ((valuesPerBit & (valuesPerBit - 1)) != 0) {
	throw std::invalid_argument("valuesPerBit in FastBitset must be a power of two");
      }
      
      numBits_ = (numValues + valuesPerBit - 1) >> shift_;
      bits_.assign((numBits_ + 63) / 64, 0);
    }

    inline __attribute__((always_inline)) size_t valueToBitIndex(size_t value) const {
      return value >> shift_;
    }

    inline __attribute__((always_inline)) bool test(size_t value) const {
      size_t bit = valueToBitIndex(value);
      return bits_[bit >> 6] & (1ULL << (bit & 63));
    }

    inline __attribute__((always_inline)) void set(size_t value) {
      size_t bit = valueToBitIndex(value);
      bits_[bit >> 6] |= (1ULL << (bit & 63));
    }

    inline __attribute__((always_inline)) bool test_and_set(size_t value) {
      size_t bit = valueToBitIndex(value);
      size_t wordIdx = bit >> 6;
      size_t mask = UINT64_C(1) << (bit & 63);
      
      size_t oldValue = bits_[wordIdx] | mask;
      return oldValue & mask;
    }

    inline __attribute__((always_inline)) void reset(size_t value) {
      size_t bit = valueToBitIndex(value);
      bits_[bit >> 6] &= ~(1ULL << (bit & 63));
    }

    inline __attribute__((always_inline)) void clear() {
      std::fill(bits_.begin(), bits_.end(), 0);
    }

    inline __attribute__((always_inline)) size_t sizeInBits() {
      return numBits_;
    }

    inline __attribute__((always_inline)) size_t size() {
      return numBits_ * valuesPerBit_;
    }

    inline __attribute__((always_inline)) void restructure(size_t numValues, size_t valuesPerBit) {
      if ((valuesPerBit & (valuesPerBit - 1)) != 0) {
	throw std::invalid_argument("valuesPerBit in FastBitset must be a power of two");
      }

      valuesPerBit_ = valuesPerBit;
      shift_ = log2_exact(valuesPerBit);
      numBits_ = (numValues + valuesPerBit - 1) >> shift_;
      bits_.assign((numBits_ + 63) / 64, 0);
    }

    inline __attribute__((always_inline)) void setRange(size_t valueStart, size_t count) {
      if (count == 0) return;

      size_t firstBit = valueToBitIndex(valueStart);
      size_t lastBit = valueToBitIndex(valueStart + count - 1);
      size_t startWord = firstBit >> 6;
      size_t endWord = lastBit >> 6;

      if (startWord < 0 || startWord >= bits_.size() ||
	  endWord < 0 || endWord >= bits_.size()) {
	throw std::runtime_error("Requested range is outside of supported ranges.");
      }

      if (startWord == endWord) {
	uint64_t mask = ((1ULL << (lastBit - firstBit + 1)) - 1) << (firstBit & 63);
	bits_[startWord] |= mask;
      } else {
	uint64_t firstMask = ~0ULL << (firstBit & 63);
	bits_[startWord++] |= firstMask;

	for (size_t w = startWord; w < endWord; ++w) {
	  bits_[w] = ~0ULL;
	}

	uint64_t lastMask = (lastBit & 63) == 63 ? ~0ULL : ((1ULL << ((lastBit & 63) + 1)) - 1);
	bits_[endWord] |= lastMask;
      }
    }

    inline __attribute__((always_inline)) bool testRange(size_t valueStart, size_t count) const {
      if (count == 0) return true;

      size_t firstBit = valueToBitIndex(valueStart);
      size_t lastBit = valueToBitIndex(valueStart + count - 1);
      size_t startWord = firstBit >> 6;
      size_t endWord = lastBit >> 6;

      if (startWord == endWord) {
	uint64_t mask = ((1ULL << (lastBit - firstBit + 1)) - 1) << (firstBit & 63);
	return (bits_[startWord] & mask) == mask;
      } else {
	uint64_t firstMask = ~0ULL << (firstBit & 63);
	if ((bits_[startWord++] & firstMask) != firstMask) return false;

	for (size_t w = startWord; w < endWord; w++) {
	  if (bits_[w] != ~0ULL) return false;
	}

	uint64_t lastMask = ((lastBit & 63) == 63) ? ~0ULL : ((1ULL << ((lastBit & 63) + 1)) - 1);
	return (bits_[endWord] & lastMask) == lastMask;
      }
    }

    inline __attribute__((always_inline)) std::pair<size_t, size_t> getValueRange(size_t value, size_t n) const {
      if (n == 0) return {value, value};

      size_t coveredStart = (value >> shift_) << shift_;
      size_t coveredEnd = (((value + n - 1) >> shift_) + 1) << shift_;
      return {coveredStart, coveredEnd};
    }

    inline __attribute__((always_inline)) std::pair<size_t, size_t> getValueRange(size_t value) const {
      size_t blockStart = (value >> shift_) << shift_;
      return {blockStart, blockStart + valuesPerBit_};
    }

    void printSetBits() const {
      for (size_t i = 0; i < numBits_; ++i) {
	size_t wordIndex = i >> 6;
	size_t bitOffset = i & 63;
	if (bits_[wordIndex] & (1ULL << bitOffset)) {
	  size_t startValue = i << shift_;
	}
      }
    }

    inline __attribute__((always_inline)) size_t numSetValues() const {
      size_t setValues = 0;
        
      for (size_t bitIndex = 0; bitIndex < numBits_; ++bitIndex) {
	size_t wordIndex = bitIndex >> 6;
	size_t bitOffset = bitIndex & 63;
            
	if (bits_[wordIndex] & (1ULL << bitOffset)) {
	  setValues++;
	}
      }
        
      return setValues;
    }

    FastBitset() = default;
    FastBitset(const FastBitset& other) = default;
    FastBitset& operator=(const FastBitset& other) = default;
    FastBitset(FastBitset&& other) noexcept = default;
    FastBitset& operator=(FastBitset&& other) noexcept = default;
    ~FastBitset() = default;
  };

  struct AtomicFastBitset {
    std::unique_ptr<std::atomic<std::uint64_t>[]> bits_;
    size_t numWords_;
    size_t valuesPerBit_;
    size_t numBits_;
    uint32_t shift_;

    explicit AtomicFastBitset(size_t numValues, size_t valuesPerBit)
      : valuesPerBit_(valuesPerBit),
        shift_(log2_exact(valuesPerBit)) {
      
      if ((valuesPerBit & (valuesPerBit - 1)) != 0) {
        throw std::invalid_argument("valuesPerBit in AtomicFastBitset must be a power of two");
      }
      
      numBits_ = (numValues + valuesPerBit - 1) >> shift_;
      numWords_ = (numBits_ + 63) / 64;
      bits_ = std::make_unique<std::atomic<std::uint64_t>[]>(numWords_);
      
      for (size_t i = 0; i < numWords_; ++i) {
        bits_[i].store(0, std::memory_order_relaxed);
      }
    }

    inline __attribute__((always_inline)) size_t valueToBitIndex(size_t value) const {
      return value >> shift_;
    }

    inline __attribute__((always_inline)) bool test(size_t value) const {
      size_t bit = valueToBitIndex(value);
      std::uint64_t word = bits_[bit >> 6].load(std::memory_order_relaxed);
      return word & (UINT64_C(1) << (bit & 63));
    }

    inline __attribute__((always_inline)) void set(size_t value) {
      size_t bit = valueToBitIndex(value);
      size_t wordIdx = bit >> 6;
      std::uint64_t mask = UINT64_C(1) << (bit & 63);
      bits_[wordIdx].fetch_or(mask, std::memory_order_relaxed);
    }
    
    inline __attribute__((always_inline)) bool test_and_set(size_t value) {
      size_t bit = valueToBitIndex(value);
      size_t wordIdx = bit >> 6;
      std::uint64_t mask = UINT64_C(1) << (bit & 63);
      
      std::uint64_t oldValue = bits_[wordIdx].fetch_or(mask, std::memory_order_acq_rel);
      return oldValue & mask;
    }

    inline __attribute__((always_inline)) void reset(size_t value) {
      size_t bit = valueToBitIndex(value);
      size_t wordIdx = bit >> 6;
      std::uint64_t mask = ~(UINT64_C(1) << (bit & 63));
      bits_[wordIdx].fetch_and(mask, std::memory_order_acq_rel);
    }

    inline __attribute__((always_inline)) void clear() {
      for (size_t i = 0; i < numWords_; ++i) {
        bits_[i].store(0, std::memory_order_relaxed);
      }
    }

    inline __attribute__((always_inline)) void restructure(size_t numValues, size_t valuesPerBit) {
      if ((valuesPerBit & (valuesPerBit - 1)) != 0) {
        throw std::invalid_argument("valuesPerBit in AtomicFastBitset must be a power of two");
      }

      valuesPerBit_ = valuesPerBit;
      shift_ = log2_exact(valuesPerBit);
      numBits_ = (numValues + valuesPerBit - 1) >> shift_;
      numWords_ = (numBits_ + 63) / 64;
      
      bits_ = std::make_unique<std::atomic<std::uint64_t>[]>(numWords_);
      
      for (size_t i = 0; i < numWords_; ++i) {
        bits_[i].store(0, std::memory_order_relaxed);
      }
    }

    inline __attribute__((always_inline)) void setRange(size_t valueStart, size_t count) {
      if (count == 0) return;

      size_t firstBit = valueToBitIndex(valueStart);
      size_t lastBit = valueToBitIndex(valueStart + count - 1);
      size_t startWord = firstBit >> 6;
      size_t endWord = lastBit >> 6;

      if (startWord >= numWords_ || endWord >= numWords_) {
        throw std::runtime_error("Requested range is outside of supported ranges.");
      }

      if (startWord == endWord) {
        std::uint64_t mask = ((UINT64_C(1) << (lastBit - firstBit + 1)) - 1) << (firstBit & 63);
        bits_[startWord].fetch_or(mask, std::memory_order_acq_rel);
      } else {
        std::uint64_t firstMask = ~UINT64_C(0) << (firstBit & 63);
        bits_[startWord].fetch_or(firstMask, std::memory_order_acq_rel);

        for (size_t w = startWord + 1; w < endWord; ++w) {
          bits_[w].store(~UINT64_C(0), std::memory_order_release);
        }

        std::uint64_t lastMask = (lastBit & 63) == 63 ? ~UINT64_C(0) : ((UINT64_C(1) << ((lastBit & 63) + 1)) - 1);
        bits_[endWord].fetch_or(lastMask, std::memory_order_acq_rel);
      }
    }

    inline __attribute__((always_inline)) bool testRange(size_t valueStart, size_t count) const {
      if (count == 0) return true;

      size_t firstBit = valueToBitIndex(valueStart);
      size_t lastBit = valueToBitIndex(valueStart + count - 1);
      size_t startWord = firstBit >> 6;
      size_t endWord = lastBit >> 6;

      if (startWord == endWord) {
        std::uint64_t mask = ((UINT64_C(1) << (lastBit - firstBit + 1)) - 1) << (firstBit & 63);
        std::uint64_t word = bits_[startWord].load(std::memory_order_acquire);
        return (word & mask) == mask;
      } else {
        std::uint64_t firstMask = ~UINT64_C(0) << (firstBit & 63);
        std::uint64_t firstWord = bits_[startWord].load(std::memory_order_acquire);
        if ((firstWord & firstMask) != firstMask) return false;

        for (size_t w = startWord + 1; w < endWord; w++) {
          if (bits_[w].load(std::memory_order_acquire) != ~UINT64_C(0)) return false;
        }

        std::uint64_t lastMask = ((lastBit & 63) == 63) ? ~UINT64_C(0) : ((UINT64_C(1) << ((lastBit & 63) + 1)) - 1);
        std::uint64_t lastWord = bits_[endWord].load(std::memory_order_acquire);
        return (lastWord & lastMask) == lastMask;
      }
    }

    inline __attribute__((always_inline)) std::pair<size_t, size_t> getValueRange(size_t value, size_t n) const {
      if (n == 0) return {value, value};
      size_t coveredStart = (value >> shift_) << shift_;
      size_t coveredEnd = (((value + n - 1) >> shift_) + 1) << shift_;
      return {coveredStart, coveredEnd};
    }

    inline __attribute__((always_inline)) std::pair<size_t, size_t> getValueRange(size_t value) const {
      size_t blockStart = (value >> shift_) << shift_;
      return {blockStart, blockStart + valuesPerBit_};
    }

    void printSetBits() const {
      for (size_t i = 0; i < numBits_; ++i) {
        size_t wordIndex = i >> 6;
        size_t bitOffset = i & 63;
        std::uint64_t word = bits_[wordIndex].load(std::memory_order_acquire);
        if (word & (UINT64_C(1) << bitOffset)) {
          size_t startValue = i << shift_;
        }
      }
    }

    inline __attribute__((always_inline)) size_t sizeInBits() const {
      return numBits_;
    }

    inline __attribute__((always_inline)) size_t size() const {
      return numBits_ * valuesPerBit_;
    }

    AtomicFastBitset() : numWords_(0), valuesPerBit_(0), numBits_(0), shift_(0) {}

    AtomicFastBitset(const AtomicFastBitset& other) 
        : numWords_(other.numWords_), valuesPerBit_(other.valuesPerBit_), 
          numBits_(other.numBits_), shift_(other.shift_) {
        if (numWords_ > 0) {
            bits_ = std::make_unique<std::atomic<std::uint64_t>[]>(numWords_);
            for (size_t i = 0; i < numWords_; ++i) {
                bits_[i].store(other.bits_[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
        }
    }

    AtomicFastBitset& operator=(const AtomicFastBitset& other) {
        if (this != &other) {
            numWords_ = other.numWords_;
            valuesPerBit_ = other.valuesPerBit_;
            numBits_ = other.numBits_;
            shift_ = other.shift_;
            
            if (numWords_ > 0) {
                bits_ = std::make_unique<std::atomic<std::uint64_t>[]>(numWords_);
                for (size_t i = 0; i < numWords_; ++i) {
                    bits_[i].store(other.bits_[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
                }
            } else {
                bits_.reset();
            }
        }
        return *this;
    }

    AtomicFastBitset(AtomicFastBitset&& other) noexcept 
        : bits_(std::move(other.bits_)), 
          numWords_(other.numWords_),
          valuesPerBit_(other.valuesPerBit_), 
          numBits_(other.numBits_), 
          shift_(other.shift_) {
        other.numWords_ = 0;
        other.valuesPerBit_ = 0;
        other.numBits_ = 0;
        other.shift_ = 0;
    }

    AtomicFastBitset& operator=(AtomicFastBitset&& other) noexcept {
        if (this != &other) {
            bits_ = std::move(other.bits_);
            numWords_ = other.numWords_;
            valuesPerBit_ = other.valuesPerBit_;
            numBits_ = other.numBits_;
            shift_ = other.shift_;
            
            other.numWords_ = 0;
            other.valuesPerBit_ = 0;
            other.numBits_ = 0;
            other.shift_ = 0;
        }
        return *this;
    }

    ~AtomicFastBitset() = default;
  };
  
  struct TableManager {

    boss::Span<int8_t> byteSpan;
    RootExpression *root;
    size_t argumentCount;

    int64_t numRows = -1;
    
    bool isInitialRun = false;
    std::unordered_set<boss::Symbol> allSymbols;
    std::unordered_map<boss::Symbol, boss::Expression> dictionaryMap;
    
    std::unordered_map<boss::Symbol, FastBitset> columnBitsets;
    boss::Symbol currColumn = "EMPTY"_;

    thread_local static std::unordered_set<boss::Symbol> columnsLoaded;
    
    FastBitset loadedArgumentsFast;
    FastBitset duplicateIndicesDetectorFast;
    
    static uint64_t alignTo8Bytes(uint64_t bytes) {
      return (bytes + (uint64_t)7) & -(uint64_t)8;
    }

    template <typename T>
    static typename std::enable_if_t<std::is_integral_v<T>, std::vector<T>>
    mergeToRange(std::vector<T>&& sortedValues, T range) {
      const size_t n = sortedValues.size();
      if (n == 0) {
	return {};
      }

      std::vector<T> res(n * 2);

      size_t writeI = 0;
      T start = sortedValues[0];
      T end = start;

      for (size_t i = 1; i < n; i++) {
	T curr = sortedValues[i];
	if (curr <= end + range) {
	  end = curr;
	} else {
	  res[writeI++] = start;
	  res[writeI++] = end;
	  start = end = curr;
	}
      }

      res[writeI++] = start;
      res[writeI++] = end;

      res.resize(writeI);
      return res;
    }

    template <typename T>
    static typename std::enable_if_t<std::is_integral_v<T>, void>
    mergeSortedFlatPairsToRange(std::vector<T>& sortedPairs, T range) {
      const size_t n = sortedPairs.size();
      if (n == 0) {
	return;
      }

      size_t writeI = 0;
      T start = sortedPairs[0];
      T end = sortedPairs[1];

      for (size_t readI = 2; readI < n; readI += 2) {
	T nextStart = sortedPairs[readI];
	T nextEnd = sortedPairs[readI+1];

	if (nextStart <= end + range) {
	  end = nextEnd;
	} else {
	  sortedPairs[writeI++] = start;
	  sortedPairs[writeI++] = end;
	  start = nextStart;
	  end = nextEnd;
	}
      }

      sortedPairs[writeI++] = start;
      sortedPairs[writeI++] = end;

      sortedPairs.resize(writeI);
    }

    template <typename T>
    static typename std::enable_if_t<std::is_integral_v<T>, void>
    removeDuplicatesFromSortedFlatPairs(std::vector<T>& sortedPairs) {
      const size_t n = sortedPairs.size();

      size_t writeI = 0;
      for (size_t readI = 0; readI < n; readI += 2) {
	if (writeI == 0 ||
	    sortedPairs[readI] != sortedPairs[writeI - 2] ||
	    sortedPairs[readI + 1] != sortedPairs[writeI - 1]) {
	  sortedPairs[writeI++] = sortedPairs[readI];
	  sortedPairs[writeI++] = sortedPairs[readI + 1];
	}
      }
      sortedPairs.resize(writeI);
    }

    template <typename S>
    typename std::enable_if_t<std::is_integral_v<S>, void>
    removeDuplicatesWithBitset(std::vector<S>& vec) {
      const size_t n = vec.size();
      std::vector<S> output(n);
      
      size_t writeI = 0;
      for (size_t readI = 0; readI < n; ++readI) {
	S val = vec[readI];
	if (!duplicateIndicesDetectorFast.test(val)) {
	  duplicateIndicesDetectorFast.set(val);
	  output[writeI++] = val;
	}
      }

      output.resize(writeI);
      vec = std::move(output);

      for (size_t i = 0; i < writeI; ++i) {
	duplicateIndicesDetectorFast.reset(vec[i]);
      }
    }

    struct PairHash {
      std::size_t operator()(const std::pair<size_t, size_t>& value) const {
        size_t h1 = std::hash<size_t>{}(value.first);
	size_t h2 = std::hash<size_t>{}(value.second);
	return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
      }
    };

    template <typename T>
    static typename std::enable_if_t<std::is_integral_v<T>, void>
    removeDuplicates(std::vector<T>& vec) {
      robin_hood::unordered_set<T> seen;
      seen.reserve(vec.size());

      size_t writeI = 0;
      for (size_t readI = 0; readI < vec.size(); readI++) {
	const T& val = vec[readI];
	if (seen.insert(val).second) {
	  vec[writeI++] = val;
	}
      }

      vec.resize(writeI);
    }
      
    static void removeDuplicates(std::vector<std::pair<size_t, size_t>>& vec) {
      robin_hood::unordered_set<std::pair<size_t, size_t>, PairHash> seen;
      seen.reserve(vec.size());
      
      size_t writeI = 0;
      for (size_t readI = 0; readI < vec.size(); readI++) {
	const auto& val = vec[readI];
	if (seen.insert(val).second) {
	  vec[writeI++] = val;
	}
      }

      vec.resize(writeI);
    };

    void updateSpan(const std::string &url, std::vector<int64_t> &bounds, EvalFunction &loader, bool trackingCache) {      
      if (bounds.size() == 0) {
	return;
      }
      auto fetchExpr = createFetchExpression(url, bounds, trackingCache, argumentCount);
      auto byteSeqExpr = std::get<boss::ComplexExpression>(applyEngine(std::move(fetchExpr), loader));
      byteSpan = std::move(getByteSequence(std::move(byteSeqExpr)));
    }

    void updateRoot(const std::string &url, std::vector<int64_t> &bounds, EvalFunction &loader, bool trackingCache) {
      updateSpan(url, bounds, loader, trackingCache);
      root = (RootExpression *)byteSpan.begin();
      *((void **)&root->originalAddress) = root;
    }

    std::vector<int64_t> getArgBounds(std::vector<size_t> const &offsets) {
      const size_t baseOffset = sizeof(RootExpression);
      const size_t argSize = sizeof(Argument);

      std::vector<int64_t> bounds(offsets.size() * 2);
      size_t outI = 0;
      size_t i = 0;

      while (i < offsets.size()) {
	size_t start = offsets[i];
	size_t end = start;

	while (i + 1 < offsets.size() && offsets[i+1] == offsets[i] + 1) {
	  i++;
	  end++;
	}

	bounds[outI++] = static_cast<int64_t>(baseOffset + start * argSize);
	bounds[outI++] = static_cast<int64_t>(baseOffset + (end + 1) * argSize - 1);
	i++;
      }

      bounds.resize(outI);
      return bounds;
    }
    
    std::vector<int64_t> getArgTypeBounds(std::vector<size_t> const &offsets) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 2);
      size_t currByteOffset = sizeof(RootExpression);
      currByteOffset += alignTo8Bytes(root->argumentBytesCount);
      for (auto offset : offsets) {
	bounds.emplace_back(currByteOffset + (offset * sizeof(ArgumentType)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(ArgumentType)) + 1);
      }

      return bounds;
    }

    std::vector<int64_t> getArgAndTypeBounds(std::vector<size_t> const &offsets) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 4);
      size_t currByteOffset = sizeof(RootExpression);
      for (auto offset : offsets) {
	bounds.emplace_back(currByteOffset + (offset * sizeof(Argument)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(Argument)) + 1);
      }
      currByteOffset += alignTo8Bytes(root->argumentBytesCount);
      for (auto offset : offsets) {
	bounds.emplace_back(currByteOffset + (offset * sizeof(ArgumentType)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(ArgumentType)) + 1);
      }

      return bounds;
    }

    std::vector<int64_t> getArgAndTypeBounds(std::vector<size_t> const &argOffsets, std::vector<size_t> const &typeOffsets) {
      std::vector<int64_t> bounds;
      bounds.reserve(argOffsets.size() * 2 + typeOffsets.size() * 2);
      size_t currByteOffset = sizeof(RootExpression);
      for (auto offset : argOffsets) {
	bounds.emplace_back(currByteOffset + (offset * sizeof(Argument)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(Argument)) + 1);
      }
      currByteOffset += alignTo8Bytes(root->argumentBytesCount);
      for (auto offset : typeOffsets) {
	bounds.emplace_back(currByteOffset + (offset * sizeof(ArgumentType)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(ArgumentType)) + 1);
      }

      return bounds;
    }

    std::vector<int64_t> getArgBoundsFromIntervals(std::vector<Range> const &intervals) {
      std::vector<int64_t> bounds(intervals.size() * 2);
      const size_t baseOffset = sizeof(RootExpression);
      const size_t scalarOffset = sizeof(Argument);
      size_t writeI = 0;
      for (size_t intervalI = 0; intervalI < intervals.size(); intervalI++) {
	auto& interval = intervals[intervalI];
	bounds[writeI++] = baseOffset + (interval.start * scalarOffset);
	bounds[writeI++] = baseOffset + ((interval.end + 1) * scalarOffset - 1);
      }
      return bounds;
    }
    
    std::vector<int64_t> getArgBoundsFromIntervals(std::vector<std::pair<size_t, size_t>> const &intervals) {
      std::vector<int64_t> bounds(intervals.size() * 2);
      const size_t baseOffset = sizeof(RootExpression);
      const size_t scalarOffset = sizeof(Argument);
      size_t writeI = 0;
      for (auto const &[start, end] : intervals) {
	bounds[writeI++] = baseOffset + (start * scalarOffset);
	bounds[writeI++] = baseOffset + ((end + 1) * scalarOffset - 1);
      }

      return bounds;
    }

    std::vector<int64_t> getArgBoundsContiguous(size_t const &startOffset,
						size_t const &size) {
      std::vector<int64_t> bounds;
      auto endOffset = startOffset + size - 1;

      size_t currByteOffset = sizeof(RootExpression);
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(Argument)) - 1);
      bounds.emplace_back(currByteOffset + ((endOffset + 1) * sizeof(Argument)) + 1);

      return bounds;
    }

    std::vector<int64_t> getArgTypeBoundsContiguous(size_t const &startOffset,
						    size_t const &size) {
      std::vector<int64_t> bounds;
      auto endOffset = startOffset + size - 1;

      size_t currByteOffset = sizeof(RootExpression);
      currByteOffset += alignTo8Bytes(root->argumentBytesCount);
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(ArgumentType)) - 1);
      bounds.emplace_back(currByteOffset + ((endOffset + 1) * sizeof(ArgumentType)) + 1);

      return bounds;
    }

    std::vector<int64_t> getArgAndTypeBoundsContiguous(size_t const &startOffset,
						       size_t const &size) {
      std::vector<int64_t> bounds;
      auto endOffset = startOffset + size - 1;

      size_t currByteOffset = sizeof(RootExpression);
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(Argument)) - 1);
      bounds.emplace_back(currByteOffset + ((endOffset + 1) * sizeof(Argument)) + 1);
      currByteOffset += alignTo8Bytes(root->argumentBytesCount);
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(ArgumentType)) - 1);
      bounds.emplace_back(currByteOffset +
			  ((endOffset + 1) * sizeof(ArgumentType)) + 1);

      return bounds;
    }
    
    std::vector<int64_t> getArgAndTypeBoundsContiguous(size_t const &startOffset,
						       size_t const &startTypeOffset,
						       size_t const &size) {
      std::vector<int64_t> bounds;
      auto endOffset = startOffset + size - 1;
      auto endTypeOffset = startTypeOffset + size - 1;

      size_t currByteOffset = sizeof(RootExpression);
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(Argument)) - 1);
      bounds.emplace_back(currByteOffset + ((endOffset + 1) * sizeof(Argument)) + 1);
      currByteOffset += alignTo8Bytes(root->argumentBytesCount);
      bounds.emplace_back(currByteOffset + (startTypeOffset * sizeof(ArgumentType)) - 1);
      bounds.emplace_back(currByteOffset +
			  ((endTypeOffset + 1) * sizeof(ArgumentType)) + 1);

      return bounds;
    }

    std::vector<int64_t> getExprBounds(std::vector<size_t> const &offsets) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 2);
      size_t currByteOffset = sizeof(RootExpression) +
	alignTo8Bytes(root->argumentBytesCount) +
	alignTo8Bytes(root->argumentCount * sizeof(ArgumentType));
      for (auto offset : offsets) {
	bounds.emplace_back(currByteOffset +
			    (offset * sizeof(SerializationExpression)) - 1);
	bounds.emplace_back(currByteOffset +
			    ((offset + 1) * sizeof(SerializationExpression)) + 1);
      }
      return bounds;
    }

    std::vector<int64_t> getStringBounds(std::vector<size_t> const &offsets,
					 size_t approxLength) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 2);
      size_t currByteOffset =
	sizeof(RootExpression) + alignTo8Bytes(root->argumentBytesCount) +
	alignTo8Bytes(root->argumentCount * sizeof(ArgumentType)) +
	root->expressionCount * sizeof(SerializationExpression);
      for (auto offset : offsets) {
	bounds.emplace_back(currByteOffset + offset - 1);
	bounds.emplace_back(currByteOffset + offset + approxLength + 1);
      }

      return bounds;
    }
  
    std::vector<size_t> requestArguments(const std::vector<size_t> &indicesOffsets) {
      if (columnBitsets.find(currColumn) == columnBitsets.end()) {
	throw std::runtime_error("Cannot call requestArguments for column without bitset cache");
      }
      
      std::vector<size_t> needed;
      needed.resize(indicesOffsets.size() * 4);

      for (auto const &offset : indicesOffsets) {
	if (!loadedArgumentsFast.test(offset)) {
	  loadedArgumentsFast.set(offset);
	  auto [start, end] = loadedArgumentsFast.getValueRange(offset);
	  for (auto i = start; i < end; i++) {
	    needed.push_back(i);
	  }
	}
      }

      return std::move(needed);
    }

    static std::vector<Range> mergeSortedRanges(std::vector<Range>& ranges, size_t maxGap = 0) {
      if (ranges.size() <= 1) return std::move(ranges);
    
      std::vector<Range> merged;
      merged.reserve(ranges.size());
    
      Range current = ranges[0];
    
      for (size_t i = 1; i < ranges.size(); ++i) {
        if (current.canMerge(ranges[i], maxGap)) {
	  current = current.merge(ranges[i]);
        } else {
	  merged.push_back(current);
	  current = ranges[i];
        }
      }
    
      merged.push_back(current);
      return merged;
    }

    std::vector<Range> optimiseForMultipart(const std::vector<Range>& ranges,
					    int32_t datasetSize = -1) {

      static constexpr int32_t MULTIPART_OVERHEAD_PER_RANGE = 140;
      static constexpr double MIN_ACCEPTABLE_EFFICIENCY = 0.4;
      static constexpr double TARGET_EFFICIENCY = 0.7;        
      static constexpr double MAX_WASTE_RATIO = 0.6;          

      if (ranges.empty()) return {};
      if (ranges.size() == 1) return ranges;

      int64_t totalUsefulValues = 0;
      for (const auto& range : ranges) {
        totalUsefulValues += range.size();
      }

      double avgRangeSize = static_cast<double>(totalUsefulValues) / ranges.size();

      int64_t baseMaxGap = static_cast<int64_t>(
						  std::min(MULTIPART_OVERHEAD_PER_RANGE * 0.5, avgRangeSize * 0.25)
						  );

      std::vector<Range> coalescedRanges;
      coalescedRanges.reserve(ranges.size());

      Range currentRange = ranges[0];
      int64_t usefulValuesInCurrent = currentRange.size();

      for (size_t i = 1; i < ranges.size(); ++i) {
        const Range& nextRange = ranges[i];
        int64_t gap = currentRange.gapTo(nextRange);

        int64_t maxAllowedGap = baseMaxGap;
        if ((usefulValuesInCurrent / 2) < maxAllowedGap) {
	  maxAllowedGap = usefulValuesInCurrent / 2;
        }
        if (nextRange.size() < maxAllowedGap) {
	  maxAllowedGap = nextRange.size();
        }

        int64_t mergedTotalSize = nextRange.end - currentRange.start;
        int64_t mergedUsefulValues = usefulValuesInCurrent + nextRange.size();
        int64_t mergedWaste = mergedTotalSize - mergedUsefulValues;

        double currentEff = static_cast<double>(usefulValuesInCurrent) / 
	  (currentRange.size() + MULTIPART_OVERHEAD_PER_RANGE);
        double nextEff = static_cast<double>(nextRange.size()) / 
	  (nextRange.size() + MULTIPART_OVERHEAD_PER_RANGE);
        double mergedEff = static_cast<double>(mergedUsefulValues) / 
	  (mergedTotalSize + MULTIPART_OVERHEAD_PER_RANGE);

        double wasteRatio = static_cast<double>(mergedWaste) / mergedTotalSize;

        bool cacheLocalityBonus = (gap > 0 && gap <= 8);
        double cpu_efficiencyBonus = cache_localityBonus ? 0.05 : 0.0;
        
        double adjustedMergedEff = mergedEff + cpu_efficiencyBonus;

        bool gapAcceptable = (gap <= maxAllowedGap);
        bool efficiencyImproved = (adjustedMergedEff > std::min(currentEff, nextEff) * 1.05);
        bool efficiencyAcceptable = (adjustedMergedEff >= MIN_ACCEPTABLE_EFFICIENCY);
        bool wasteAcceptable = (wasteRatio <= MAX_WASTE_RATIO);

        bool shouldMerge = gapAcceptable && efficiencyImproved && 
	  efficiencyAcceptable && wasteAcceptable;

        if (shouldMerge) {
	  currentRange.end = nextRange.end;
	  usefulValuesInCurrent += nextRange.size();
        } else {
	  coalescedRanges.push_back(currentRange);
	  currentRange = nextRange;
	  usefulValuesInCurrent = nextRange.size();
        }
      }

      coalescedRanges.push_back(currentRange);

      return coalescedRanges;
    }
 
    template<typename T>
    std::vector<Range> requestArgumentsPairedVectorisedSingleThreaded(const std::vector<T> &indicesOffsets, FastBitset& columnBitset, size_t startChildOffset = 0, size_t gap = 1, size_t dataset_size = -1) {

      const size_t numIndices = indicesOffsets.size();
      std::vector<Range> needed;
      needed.reserve(numIndices >> 2);

      size_t i = 0;
      while (i < numIndices) {
	const T firstIndex = indicesOffsets[i];

	if (columnBitset.test(firstIndex)) {
	  i++;
	  continue;
	}

	size_t rangeStart = i;
	T lastIndex = firstIndex;

	while (++i < numIndices) {
	  const T nextIndex = indicesOffsets[i];
	  if (nextIndex - lastIndex > gap) break;
	  if (nextIndex - lastIndex > gap || columnBitset.test(nextIndex)) break;
	  lastIndex = nextIndex;
	}

	columnBitset.setRange(firstIndex, lastIndex - firstIndex + 1);
        
	auto [blockStart, _unused] = columnBitset.getValueRange(indicesOffsets[rangeStart]);
	auto [_unused2, blockEnd] = columnBitset.getValueRange(lastIndex);

	const Range newRange{
	  static_cast<int32_t>(blockStart + startChildOffset),
	  static_cast<int32_t>(blockEnd + startChildOffset) 
	};
	
	if (!needed.empty() && needed.back().canMerge(newRange, gap)) {
	  needed.back() = needed.back().merge(newRange);
	} else {
	  needed.push_back(newRange);
	}

	columnBitset.setRange(indicesOffsets[rangeStart], lastIndex - indicesOffsets[rangeStart] + 1);
      }

      
      return optimiseForMultipart(needed, dataset_size);
    }
    
    
    template<typename T>
    std::vector<Range> requestArgumentsPaired(const std::vector<T> &indicesOffsets, size_t numThreadsIn, size_t startChildOffset = 0, size_t gap = 1, size_t dataset_size = -1) {
      if (columnBitsets.find(currColumn) == columnBitsets.end()) {
	throw std::runtime_error("Cannot request arguments for column that is not yet cached.");
      }
      auto& columnBitset = columnBitsets[currColumn];
      
      const size_t numIndices = indicesOffsets.size();
      if (numIndices == 0) return {};

      std::vector<Range> needed;

      if (numIndices < THREADING_THRESHOLD / 100 || numThreadsIn == 1) {
	needed = std::move(requestArgumentsPairedVectorisedSingleThreaded(indicesOffsets, columnBitset, startChildOffset, gap, dataset_size));
      } else {
	const int numThreads = std::min(static_cast<int>(numThreadsIn), 
					static_cast<int>((numIndices + 1023) / 1024));
    
	std::vector<std::vector<Range>> threadResults(numThreads);

#pragma omp parallel num_threads(numThreads)
	{
	  const int threadId = omp_get_thread_num();
	  const size_t chunkSize = (numIndices + numThreads - 1) / numThreads;
	  const size_t startIdx = threadId * chunkSize;
	  const size_t endIdx = std::min(startIdx + chunkSize, numIndices);
      
	  auto& localNeeded = threadResults[threadId];
	  localNeeded.reserve(chunkSize);
      
	  for (size_t i = startIdx; i < endIdx; ++i) {
	    const auto& offset = indicesOffsets[i];
        
	    if (!columnBitset.test_and_set(offset)) {
	      auto [tempS, tempE] = columnBitset.getValueRange(offset);
	      localNeeded.emplace_back(tempS + startChildOffset, tempE + startChildOffset);
	    }
	  }
	}
    
	size_t totalSize = 0;
	for (const auto& threadResult : threadResults) {
	  totalSize += threadResult.size();
	}
    
	needed.reserve(totalSize);
	for (auto& threadResult : threadResults) {
	  needed.insert(needed.end(), 
			std::make_move_iterator(threadResult.begin()),
			std::make_move_iterator(threadResult.end()));
	}
    
	if (!needed.empty() && needed.size() > 1) {
      
	  size_t writeI = 0;
	  for (size_t readI = 1; readI < needed.size(); ++readI) {
	    if (needed[writeI].end + gap >= needed[readI].start) {
	      int32_t oldEnd = needed[writeI].end;
	      needed[writeI].end = std::max(needed[writeI].end, needed[readI].end);
          
	      if (needed[readI].start > oldEnd + 1) {
		columnBitset.setRange(oldEnd + 1 - startChildOffset, 
				      needed[readI].start - oldEnd - 1);
	      }
	      if (needed[readI].end > oldEnd) {
		columnBitset.setRange(std::max(oldEnd + 1, needed[readI].start) - startChildOffset,
				      needed[readI].end - std::max(oldEnd, needed[readI].start - 1));
	      }
	    } else {
	      ++writeI;
	      if (writeI != readI) {
		needed[writeI] = needed[readI];
	      }
	    }
	  }
	  needed.resize(writeI + 1);
	}
      }

      return needed;
    }
    
    std::vector<std::pair<size_t, size_t>> requestArgumentRanges(const std::vector<std::pair<size_t, size_t>> &intervalOffsets) {
      std::vector<std::pair<size_t, size_t>> needed;
      needed.reserve(intervalOffsets.size());
      std::vector<std::pair<size_t, size_t>> realNeeded;
      realNeeded.reserve(intervalOffsets.size());

      size_t sum = 0;
      size_t sumNeeded = 0;
      for (auto const &[start, end] : intervalOffsets) {
	sum += (end - start + 1);
	if (!loadedArgumentsFast.testRange(start, (end - start + 1))) {
	  realNeeded.emplace_back(start, end);
	}
	for (size_t i = start; i < end + 1; i++) {
	  if (!loadedArgumentsFast.test(i)) {
	    sumNeeded++;
	  }
	}
	if (!loadedArgumentsFast.test(start) && !loadedArgumentsFast.test(end)) {
	  loadedArgumentsFast.setRange(start, (end - start + 1));
	  needed.emplace_back(start, end);
	}
      }
      return std::move(needed);
    }

    template<typename T>
    std::vector<std::pair<size_t, size_t>> requestArgumentRanges(const std::vector<T> &intervalOffsets) {
      std::vector<std::pair<size_t, size_t>> needed;
      needed.reserve(intervalOffsets.size() / 2);

      for (size_t i = 0; i < intervalOffsets.size(); i += 2) {
	const auto& start = intervalOffsets[i];
	const auto& end = intervalOffsets[i+1];
	if (!loadedArgumentsFast.test(start) && !loadedArgumentsFast.test(end)) {
	  loadedArgumentsFast.setRange(start, (end - start + 1));
	  needed.emplace_back(start, end);
	}
      }
      return std::move(needed);
    }

    void markArgumentsContiguous(size_t startOffset, size_t size) {
      if (columnBitsets.find(currColumn) == columnBitsets.end()) {
	return;
      }
      auto& columnBitset = columnBitsets[currColumn];
      columnBitset.setRange(startOffset, size);
    }

    std::vector<size_t> inflateVector(const std::vector<size_t>& input, size_t inflateRange) {
      std::vector<size_t> result;
      if (input.empty()) return result;

      result.reserve(input.size() * (inflateRange + 1));
      auto last = input.back();
      for (size_t i = 0; i < input.size(); ++i) {
        size_t start = input[i];
        size_t end;

        if (i + 1 < input.size() && input[i + 1] - start <= inflateRange) {
	  end = input[i + 1];
        } else {
	  end = start + inflateRange;
        }

        if (!result.empty() && start <= result.back()) {
	  start = result.back() + 1;
        }

        for (size_t v = start; v <= end; ++v) {
	  if (v < last)
	    result.push_back(v);
        }
      }

      return result;
    }

    template<typename T>
    std::vector<std::pair<size_t, size_t>> mergeInRange(const std::vector<T> &sortedOffsets, size_t range) {
      const size_t n = sortedOffsets.size();
      if (n == 0) {
	std::vector<std::pair<size_t, size_t>> res;
	return res;
      }
      
      std::vector<std::pair<size_t, size_t>> res(n);
      size_t writeI = 0;
      size_t start = sortedOffsets[0];
      size_t end = start;

      for (size_t i = 1; i < n; i++) {
	size_t curr = sortedOffsets[i];
	if (curr <= end + range) {
	  end = curr;
	} else {
	  res[writeI++] = {start, end};
	  start = end = curr;
	}
      }

      res[writeI++] = {start, end};

      res.resize(writeI);
      return res;
    }

    template<typename T>
    void loadArgOffsetsRanged(const std::string &url, const std::vector<T> &offsets, EvalFunction &loader) {
      auto totalRanges = CHANGEABLE_RANGES;
      std::vector<int64_t> argBounds;
      auto neededIntervals = requestArgumentRanges(offsets);
      argBounds = getArgBoundsFromIntervals(neededIntervals);
      updateRoot(url, argBounds, loader, false);
    }

    template<typename T>
    void loadArgOffsets(const std::string &url, const std::vector<T> &offsets, EvalFunction &loader, size_t numThreads, size_t startChildOffset = 0, size_t numChildren = 0) {
      auto totalRanges = CHANGEABLE_RANGES;
      std::vector<int64_t> argBounds;
      std::vector<Range> needed;
      needed = std::move(requestArgumentsPaired(offsets, numThreads, startChildOffset, 32, numChildren));
      argBounds = getArgBoundsFromIntervals(needed);
      updateRoot(url, argBounds, loader, false);
    }

    void loadArgOffsets(const std::string &url, const std::vector<std::pair<size_t, size_t>> &intervals, EvalFunction &loader) {
      auto neededIntervals = requestArgumentRanges(intervals);
      auto argBounds = getArgBoundsFromIntervals(neededIntervals);
      updateRoot(url, argBounds, loader, false);
    }
    
    void loadArgAndTypeOffsets(const std::string &url, const std::vector<size_t> &offsets, EvalFunction &loader) {
      auto neededOffsets = requestArguments(offsets);
      auto argBounds = getArgAndTypeBounds(neededOffsets);
      updateRoot(url, argBounds, loader, true);
    }
    
    void loadArgAndTypeOffsets(const std::string &url, const std::vector<size_t> &argOffsets,
			       const std::vector<size_t> &typeOffsets, EvalFunction &loader) {
      auto neededOffsets = requestArguments(argOffsets);
      auto argBounds = getArgBounds(neededOffsets);
      auto typeBounds = getArgTypeBounds(typeOffsets);
      updateRoot(url, argBounds, loader, true);
      updateRoot(url, typeBounds, loader, true);
    }

    void loadArgOffsetsContiguous(const std::string &url, size_t startOffset, size_t size, EvalFunction &loader, bool markArgs = false) {
      if (markArgs) {
	markArgumentsContiguous(startOffset, size);
      }
      std::vector<int64_t> argBounds;
      if (columnBitsets.find(currColumn) == columnBitsets.end() || !markArgs) {
        argBounds = getArgBoundsContiguous(startOffset, size);
      } else {
	auto& columnBitset = columnBitsets[currColumn];
	auto [realCoveredStart, realCoveredEnd] = columnBitset.getValueRange(startOffset, size);
	size_t realCoveredSize = realCoveredEnd - realCoveredStart + 1;
	argBounds = getArgBoundsContiguous(realCoveredStart, realCoveredSize);
      }
      updateRoot(url, argBounds, loader, true);
    }

    void loadArgAndTypeOffsetsContiguous(const std::string &url, size_t startOffset, size_t size, EvalFunction &loader) {
      markArgumentsContiguous(startOffset, size);
      auto [realCoveredStart, realCoveredEnd] = loadedArgumentsFast.getValueRange(startOffset, size);
      size_t realCoveredSize = realCoveredEnd - realCoveredStart + 1;
      auto argBounds = getArgTypeBoundsContiguous(realCoveredStart, realCoveredSize);
      updateRoot(url, argBounds, loader, true);
    }

    void loadArgAndTypeOffsetsContiguous(const std::string &url, size_t startArgOffset, size_t startTypeOffset, size_t argSize, size_t typeSize, EvalFunction &loader) {
      markArgumentsContiguous(startArgOffset, argSize);
      auto [realCoveredArgStart, realCoveredArgEnd] = loadedArgumentsFast.getValueRange(startArgOffset, argSize);
      size_t realCoveredArgSize = realCoveredArgEnd - realCoveredArgStart + 1;
      auto argBounds = getArgBoundsContiguous(realCoveredArgStart, realCoveredArgSize);
      auto typeBounds = getArgTypeBoundsContiguous(startTypeOffset, typeSize);
      updateRoot(url, argBounds, loader, true);
      updateRoot(url, typeBounds, loader, true);
    }

    // FOR RANGED INDICES
    void loadArgOffsetsContiguous(const std::string &url, std::vector<size_t> startOffsets, std::vector<size_t> sizes, EvalFunction &loader) {
      if (startOffsets.size() != sizes.size()) {
	return;
      }
      std::vector<int64_t> argBounds;
      for (auto i = 0; i < startOffsets.size(); i++) {
	auto startOffset = startOffsets[i];
	auto size = sizes[i];
	markArgumentsContiguous(startOffset, size);
	auto tempBounds = getArgBoundsContiguous(startOffset, size);
        argBounds.reserve(argBounds.size() + tempBounds.size());
        argBounds.insert(argBounds.end(),
                         std::make_move_iterator(tempBounds.begin()),
                         std::make_move_iterator(tempBounds.end()));
      }
      updateRoot(url, argBounds, loader, true);
    }

    // FOR RANGED INDICES
    void loadArgAndTypeOffsetsContiguous(const std::string &url, std::vector<size_t> startOffsets, std::vector<size_t> sizes, EvalFunction &loader) {
      if (startOffsets != sizes) {
	return;
      }
      std::vector<int64_t> argBounds;
      for (auto i = 0; i < startOffsets.size(); i++) {
	auto startOffset = startOffsets[i];
	auto size = sizes[i];
	markArgumentsContiguous(startOffset, size);
	auto tempBounds = getArgAndTypeBoundsContiguous(startOffset, size);
        argBounds.reserve(argBounds.size() + tempBounds.size());
        argBounds.insert(argBounds.end(),
                         std::make_move_iterator(tempBounds.begin()),
                         std::make_move_iterator(tempBounds.end()));
      }
      updateRoot(url, argBounds, loader, true);
    }

    void loadStringOffsets(const std::string &url, const std::vector<size_t> &offsets, size_t approxLength, EvalFunction &loader) {
      auto strBounds = getStringBounds(offsets, approxLength);
      updateRoot(url, strBounds, loader, true);
    }

    void loadExprOffsets(const std::string &url, const std::vector<size_t> &offsets, EvalFunction &loader) {
      auto exprBounds = getExprBounds(offsets);
      updateRoot(url, exprBounds, loader, true);
    }

    void loadBounds(const std::string &url, std::vector<int64_t> &bounds, EvalFunction &loader, bool trackingCache) {
      updateRoot(url, bounds, loader, trackingCache);
    }

    bool currColumnHasBeenLoaded() {
      bool loaded = columnsLoaded.find(currColumn) != columnsLoaded.end();
      return loaded;
    }
    
    bool columnHasBeenLoaded(boss::Symbol currColumn) {
      bool loaded = columnsLoaded.find(currColumn) != columnsLoaded.end();
      return loaded;
    }

    void markColumnAsLoaded(boss::Symbol currColumn) {
      columnsLoaded.insert(currColumn);
    }

    void initColumnBitsets(const std::vector<boss::Symbol>& columns, size_t valsPerBit, bool setAll = false) {
      for (auto const& colHead : columns) {
	if (columnBitsets.find(colHead) == columnBitsets.end()) {
	  FastBitset colBitset;
	  colBitset.restructure(numRows, valsPerBit);
	  if (setAll) {
	    colBitset.setRange(0, numRows);
	  }
	  columnBitsets.emplace(colHead, std::move(colBitset));
	}
      }
    }

    void init(const std::string &url, EvalFunction &loader, const std::unordered_set<boss::Symbol>& newAllSymbols) {

      isInitialRun = true;
      allSymbols = newAllSymbols;
      std::vector<int64_t> metadataBounds = {0, sizeof(RootExpression)};
      updateRoot(url, metadataBounds, loader, true);
      argumentCount = root->argumentBytesCount / sizeof(Argument);
      loadedArgumentsFast.restructure(argumentCount, 8);

      duplicateIndicesDetectorFast.restructure(argumentCount, 8);
      
      uint64_t exprCount = root->expressionCount;
      auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
	      
      std::vector<size_t> exprBuffOffsets(exprCount);
      std::iota(exprBuffOffsets.begin(), exprBuffOffsets.end(), 0);

      loadExprOffsets(url, exprBuffOffsets, loader);

      auto exprBufferPtr = expr.expressionsBuffer();
      std::vector<size_t> headsOffsets;
      headsOffsets.reserve(exprCount);
      for (auto offset : exprBuffOffsets) {
	headsOffsets.emplace_back(exprBufferPtr[offset].symbolNameOffset);
      }
      loadStringOffsets(url, headsOffsets, 64, loader);

      size_t totalBounds = 0;
      std::vector<std::vector<int64_t>> offsetsToLoad;
      offsetsToLoad.reserve(exprCount * 3);
      std::vector<size_t> headIdxsToLoad(exprBuffOffsets.size());
      std::vector<size_t> dictIdxsToLoad(exprBuffOffsets.size());
      std::vector<size_t> listIdxsToLoad(exprBuffOffsets.size());
      size_t headI = 0;
      size_t dictEncListI = 0;
      size_t dictI = 0;
      size_t listI = 0;
      size_t tableStartChildOffset = 0;
      size_t tableEndChildOffset = 0;
      for (auto offset : exprBuffOffsets) {
	auto currExpr = exprBufferPtr[offset];
	auto head = boss::Symbol(viewString(root, currExpr.symbolNameOffset));
      	auto const &startArgOffset = currExpr.startChildOffset;
	auto const &endArgOffset = currExpr.endChildOffset;
	auto const &startArgTypeOffset = currExpr.startChildTypeOffset;
	auto const &endArgTypeOffset = currExpr.endChildTypeOffset;
	
	if (head != "List"_) {
	  auto const numArguments = endArgOffset - startArgOffset;
	  auto const numChildren = endArgTypeOffset - startArgTypeOffset;
	  auto argBounds = getArgBoundsContiguous(startArgOffset, numArguments);
	  auto typeBounds = getArgTypeBoundsContiguous(startArgTypeOffset, numChildren);

	  totalBounds += argBounds.size() + typeBounds.size();
	  offsetsToLoad.push_back(std::move(argBounds));
	  offsetsToLoad.push_back(std::move(typeBounds));
	} else {
	  auto neededFirstListArgBounds = getArgBounds({startArgOffset});
	  auto neededFirstListTypeBounds = getArgTypeBounds({startArgTypeOffset});

	  totalBounds += neededFirstListArgBounds.size() + neededFirstListTypeBounds.size();
	  offsetsToLoad.push_back(std::move(neededFirstListArgBounds));
	  offsetsToLoad.push_back(std::move(neededFirstListTypeBounds));
	}
      }
      auto neededExprArgBounds = getArgBounds(exprBuffOffsets);
      auto neededExprArgTypeBounds = getArgTypeBounds(exprBuffOffsets);
      totalBounds += neededExprArgBounds.size() + neededExprArgTypeBounds.size();
      offsetsToLoad.push_back(std::move(neededExprArgBounds));
      offsetsToLoad.push_back(std::move(neededExprArgTypeBounds));

      std::vector<int64_t> flattenedOffsetsToLoad;
      flattenedOffsetsToLoad.reserve(totalBounds);
      for (auto const& offVec : offsetsToLoad) {
	for (auto& off : offVec) {
	  flattenedOffsetsToLoad.push_back(off);
	}
      }
      loadBounds(url, flattenedOffsetsToLoad, loader, true);
    }
    
    explicit TableManager(const std::string &url, EvalFunction &loader, const std::unordered_set<boss::Symbol>& allSymbols) {
      init(url, loader, allSymbols);
    }

    explicit TableManager(const std::string &url, EvalFunction &loader) {
      std::unordered_set<boss::Symbol> emptySymbols;
      init(url, loader, emptySymbols);
    }

    TableManager(TableManager const&) = delete;
    TableManager &operator=(TableManager const&) = delete;
    
    TableManager(TableManager &&other) noexcept
      : argumentCount(other.argumentCount), numRows(other.numRows), isInitialRun(other.isInitialRun)
    {
      byteSpan = std::move(other.byteSpan);
      root = (RootExpression *)byteSpan.begin();
      *((void **)&root->originalAddress) = root;
      currColumn = std::move(other.currColumn);
      columnsLoaded = std::move(other.columnsLoaded);
      columnBitsets = std::move(other.columnBitsets);
      dictionaryMap = std::move(other.dictionaryMap);
      allSymbols = std::move(other.allSymbols);
      loadedArgumentsFast = std::move(other.loadedArgumentsFast);
      duplicateIndicesDetectorFast = std::move(other.duplicateIndicesDetectorFast);
      other.root = nullptr;
      other.argumentCount = 0;
    };
    TableManager &operator=(TableManager &&other) noexcept {
      if (this != &other) {
	byteSpan = std::move(other.byteSpan);
	root = (RootExpression *)byteSpan.begin();
	*((void **)&root->originalAddress) = root;
	argumentCount = other.argumentCount;
	numRows = other.numRows;
	isInitialRun = other.isInitialRun;
	currColumn = std::move(other.currColumn);
	columnsLoaded = std::move(other.columnsLoaded);
	columnBitsets = std::move(other.columnBitsets);
	dictionaryMap = std::move(other.dictionaryMap);
	allSymbols = std::move(other.allSymbols);
	loadedArgumentsFast = std::move(other.loadedArgumentsFast);
	duplicateIndicesDetectorFast = std::move(other.duplicateIndicesDetectorFast);
	other.root = nullptr;
	other.argumentCount = 0;
	other.numRows = 0;
      }
      return *this;
    };
    TableManager() = defult;
    ~TableManager() = default;
  };

  template<IndicesInt T>
  struct IndicesManager {

    std::string name;

    FastBitset deduplicationBitset;

    std::vector<T> indices;
    std::vector<T> sortedIndicesInt64FOR;
    std::vector<T> sortedIndicesInt64;
    std::vector<T> sortedIndicesInt32;
    std::vector<T> sortedIndicesInt16;
    std::vector<T> sortedIndicesInt8;

    std::unordered_map<ArgumentType, std::unordered_map<int32_t, std::vector<std::pair<T, T>>>> minimisedIntervalsMap;
    size_t numIndices;
    bool isRanged;
    bool isSet;
    bool isValid;
    
    bool duplicatesPossible;
    bool unsortedPossible;
    bool unsortedPossibleInt64;
    bool unsortedPossibleInt32;
    bool unsortedPossibleInt16;
    bool unsortedPossibleInt8;
    bool useAsRanges;
    
    size_t countIndices(const ExpressionSpanArguments &indices) {
      return std::accumulate(
			     indices.begin(), indices.end(), (size_t)0,
			     [](size_t runningSum, auto const &argument) {
			       return runningSum +
				 std::visit([&](auto const& typedSpan) {
				   return typedSpan.size();
				 }, argument);
			     });
    } 
    
    bool isValidIndexType(const ExpressionSpanArguments &indices) {
      if (indices.empty()) {
	return false;
      }
      bool isConstInt64 =
	std::holds_alternative<boss::Span<int64_t const>>(indices[0]) &&
	get<boss::Span<int64_t const>>(indices[0]).size() > 0;
      bool isInt64 = std::holds_alternative<boss::Span<int64_t>>(indices[0]) &&
	get<boss::Span<int64_t>>(indices[0]).size() > 0;
      bool isConstInt32 = std::holds_alternative<boss::Span<int32_t const>>(indices[0]) &&
	get<boss::Span<int32_t const>>(indices[0]).size() > 0;
      bool isInt32 = std::holds_alternative<boss::Span<int32_t>>(indices[0]) &&
	get<boss::Span<int32_t>>(indices[0]).size() > 0;
      bool isConstInt16 = std::holds_alternative<boss::Span<int16_t const>>(indices[0]) &&
	get<boss::Span<int16_t const>>(indices[0]).size() > 0;
      bool isInt16 = std::holds_alternative<boss::Span<int16_t>>(indices[0]) &&
	get<boss::Span<int16_t>>(indices[0]).size() > 0;
      bool isConstInt8 = std::holds_alternative<boss::Span<int8_t const>>(indices[0]) &&
	get<boss::Span<int8_t const>>(indices[0]).size() > 0;
      bool isInt8 = std::holds_alternative<boss::Span<int8_t>>(indices[0]) &&
	get<boss::Span<int8_t>>(indices[0]).size() > 0;
      return isConstInt64 || isInt64 || isConstInt32 || isInt32 || isConstInt16 || isInt16 || isConstInt8 || isInt8;
    }
    
    std::vector<T> getIndicesFromSpanArgs(const ExpressionSpanArguments &indices) {
      size_t numIndices = countIndices(indices);
      std::vector<T> res;
      res.reserve(numIndices);
      std::for_each(
		    indices.begin(), indices.end(),
		    [&res](const auto &span) {
		      std::visit([&](const auto &typedSpan) {
			auto spanSize = typedSpan.size();
		        const auto& arg0 = typedSpan[0];
			using ArgType = std::decay_t<decltype(arg0)>;
			if constexpr(std::is_same_v<ArgType, int64_t> ||
				     std::is_same_v<ArgType, int32_t> ||
				     std::is_same_v<ArgType, int16_t> ||
				     std::is_same_v<ArgType, int8_t>) {
			  res.insert(res.end(), typedSpan.begin(), typedSpan.end());
			} else {
			  assert(sizeof(ArgType) == 0);
			}
			
		      }, span);
		    });
    
      return std::move(res);
    }

    std::vector<T> getIndicesFromSpanArgsRanged(const ExpressionSpanArguments &indices) {
      size_t numIndices = countIndices(indices);
      std::vector<T> res;
      res.reserve(numIndices);

      for (size_t i = 0; i < indices.size(); i += 2) {
	if (std::holds_alternative<boss::Span<int64_t const>>(indices[i])) {
	  auto &starts = get<boss::Span<int64_t const>>(indices[i]);
	  auto &ends = get<boss::Span<int64_t const>>(indices[i + 1]);
	  if (starts.size() != ends.size()) {
	    std::cerr << "INCORRECT SIZING IN RANGE SPANS" << std::endl;
	  }
	  for (size_t j = 0; j < starts.size(); j++) {
	    auto &start = starts[j];
	    auto &end = ends[j];
	    size_t size = end - start;
	    res.emplace_back(start);
	    res.emplace_back(size);
	  }
	  
	} else if (std::holds_alternative<boss::Span<int64_t>>(indices[i])) {
	  auto &starts = get<boss::Span<int64_t>>(indices[i]);
	  auto &ends = get<boss::Span<int64_t>>(indices[i + 1]);
	  if (starts.size() != ends.size()) {
	    std::cerr << "INCORRECT SIZING IN RANGE SPANS" << std::endl;
	  }
	  for (size_t j = 0; j < starts.size(); j++) {
	    auto &start = starts[j];
	    auto &end = ends[j];
	    size_t size = end - start;
	    res.emplace_back(start);
	    res.emplace_back(size);
	  }
	}
      }

      return std::move(res);
    }

    size_t countIndicesVector() {
      if (isRanged) {
	size_t totalIndicesCount = 0;
	for (auto i = 1; i < indices.size(); i += 2) {
	  totalIndicesCount += indices[i];
	}
	return totalIndicesCount;
      }
      return indices.size();
    }

    void setIndicesFromSpans(ExpressionSpanArguments &&indicesSpans) {
      if (isRanged) {
	indices = std::move(getIndicesFromSpanArgsRanged(indicesSpans));
      } else {
	indices = std::move(getIndicesFromSpanArgs(indicesSpans));
      }
    }

    void checkIsValid(size_t total, double fraction) {
      isValid = numIndices < total * fraction;
    }

    explicit IndicesManager(ExpressionSpanArguments &&indicesSpans, bool isRanged, int64_t maxIndex = -1, int64_t bitsPerVal = -1, bool duplicatesPossible = true, std::string name = "DEFAULT", bool unsortedPossible = false) : isRanged(isRanged), duplicatesPossible(duplicatesPossible), name(name), unsortedPossible(unsortedPossible) {
      if (indicesSpans.empty()) {
	isSet = false;
	numIndices = 0;
      } else {
	if (maxIndex >= 0 && bitsPerVal > 0 && isValidIndexType(indicesSpans)) {
	  setIndicesFromSpans(std::move(indicesSpans));
	  deduplicationBitset.restructure(maxIndex, bitsPerVal);
	  isSet = true;
	  isValid = true;
	  numIndices = countIndicesVector();

	  unsortedPossibleInt64 = unsortedPossible;
	  unsortedPossibleInt32 = unsortedPossible;
	  unsortedPossibleInt16 = unsortedPossible;
	  unsortedPossibleInt8 = unsortedPossible;

	  useAsRanges = indices.size() >= CHANGEABLE_RANGES;
	  useAsRanges = false;
	}
      }
    }

    template <typename S>
    typename std::enable_if_t<std::is_integral_v<S>, void>
    removeDuplicatesWithBitset(std::vector<S>& vec) {
      const size_t n = vec.size();
      std::vector<S> output(n);
      
      size_t writeI = 0;
      for (size_t readI = 0; readI < n; ++readI) {
	S val = vec[readI];
	if (!deduplicationBitset.test(val)) {
	  deduplicationBitset.set(val);
	  output[writeI++] = val;
	}
      }

      output.resize(writeI);  
      vec = std::move(output);

      for (size_t i = 0; i < writeI; ++i) {
	deduplicationBitset.reset(vec[i]);
      }
    }

    template<typename LazyT>
    typename std::enable_if_t<std::is_integral_v<LazyT>, std::vector<T>*>
    lazilyGetDeduplicatedIndices() {
      
      if constexpr(std::is_same_v<LazyT, int64_t>) {
	if (sortedIndicesInt64.size() > 0) {
	  return &sortedIndicesInt64;
	}
	sortedIndicesInt64.resize(indices.size());
	std::copy(indices.begin(), indices.end(), sortedIndicesInt64.begin());
	if (duplicatesPossible) {
	  removeDuplicatesWithBitset(sortedIndicesInt64);
	}

	if (useAsRanges) {
	  if (unsortedPossibleInt64) {
	    std::sort(sortedIndicesInt64.begin(), sortedIndicesInt64.end());
	    unsortedPossibleInt64 = false;
	    unsortedPossibleInt32 = false;
	    unsortedPossibleInt8 = false;
	  }
	  T range = 8;
	  sortedIndicesInt64 = TableManager::mergeToRange(std::move(sortedIndicesInt64), range);
	}
	return &sortedIndicesInt64;
      } else if constexpr(std::is_same_v<LazyT, int32_t>) {
	if (sortedIndicesInt32.size() > 0) {
	  return &sortedIndicesInt32;
	}
	bool fromBase = true;
	std::vector<T>* indicesBase = &indices;
	if (sortedIndicesInt64.size() > 0) {
	  indicesBase = &sortedIndicesInt64;
	  fromBase = false;
	}

	constexpr int32_t divisor = sizeof(Argument) / sizeof(int32_t);
	constexpr uint32_t shiftAmt = log2_exact(divisor);
        
	sortedIndicesInt32.resize(indicesBase->size());
	const size_t int32N = sortedIndicesInt32.size();
	const auto &indicesBaseVec = *indicesBase;
	if (int32N > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	  for (size_t i = 0; i < int32N; i++) {
	    sortedIndicesInt32[i] = indicesBaseVec[i] >> shiftAmt;
	  }
	} else if (int32N > SIMD_THRESHOLD) {
#pragma omp simd
	  for (size_t i = 0; i < int32N; i++) {
	    sortedIndicesInt32[i] = indicesBaseVec[i] >> shiftAmt;
	  }
	} else {
	  for (size_t i = 0; i < int32N; i++) {
	    sortedIndicesInt32[i] = indicesBaseVec[i] >> shiftAmt;
	  }	
	}
	
	if (fromBase) {
	  removeDuplicatesWithBitset(sortedIndicesInt32);

	  if (useAsRanges) {
	    if (unsortedPossibleInt32) {
	      std::sort(sortedIndicesInt32.begin(), sortedIndicesInt32.end());
	      unsortedPossibleInt32 = false;
	      unsortedPossibleInt8 = false;
	    }
	    T range = 8;
	    sortedIndicesInt32 = TableManager::mergeToRange(std::move(sortedIndicesInt32), range);
	  }
	} else {
	  if (useAsRanges) {
	    T range = 8;
	    TableManager::mergeSortedFlatPairsToRange(sortedIndicesInt32, range);
	  } else {
	    removeDuplicatesWithBitset(sortedIndicesInt32);
	  }
	}
	return &sortedIndicesInt32;
      } else if constexpr(std::is_same_v<LazyT, int16_t>) {
	if (sortedIndicesInt16.size() > 0) {
	  return &sortedIndicesInt16;
	}
	bool fromBase = true;
	bool fromInt32 = false;
	std::vector<T>* indicesBase = &indices;
	if (sortedIndicesInt32.size() > 0) {
	  indicesBase = &sortedIndicesInt32;
	  fromBase = false;
	  fromInt32 = true;
	} else if (sortedIndicesInt64.size() > 0) {
	  indicesBase = &sortedIndicesInt64;
	  fromBase = false;
	}
	
	constexpr int32_t divisor = sizeof(Argument) / sizeof(int16_t);
	constexpr uint32_t shiftAmt = log2_exact(divisor);
	constexpr uint32_t halfShiftAmt = shiftAmt / 2;
	
	sortedIndicesInt16.resize(indicesBase->size());
	const size_t int16N = sortedIndicesInt16.size();
	const auto &indicesBaseVec = *indicesBase;
	if (fromInt32) {
	  if (int16N > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	    for (size_t i = 0; i < int16N; i++) {
	      sortedIndicesInt16[i] = indicesBaseVec[i] >> halfShiftAmt;
	    }
	  } else if (int16N > SIMD_THRESHOLD) {
#pragma omp simd
	    for (size_t i = 0; i < int16N; i++) {
	      sortedIndicesInt16[i] = indicesBaseVec[i] >> halfShiftAmt;
	    }
	  } else {
	    for (size_t i = 0; i < int16N; i++) {
	      sortedIndicesInt16[i] = indicesBaseVec[i] >> halfShiftAmt;
	    }
	  }
	} else {
	  if (int16N > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	    for (size_t i = 0; i < int16N; i++) {
	      sortedIndicesInt16[i] = indicesBaseVec[i] >> shiftAmt;
	    }
	  } else if (int16N > SIMD_THRESHOLD) {
#pragma omp simd
	    for (size_t i = 0; i < int16N; i++) {
	      sortedIndicesInt16[i] = indicesBaseVec[i] >> shiftAmt;
	    }
	  } else {
	    for (size_t i = 0; i < int16N; i++) {
	      sortedIndicesInt16[i] = indicesBaseVec[i] >> shiftAmt;
	    }	  
	  }
	}
	
	if (fromBase) {
	  removeDuplicatesWithBitset(sortedIndicesInt16);
	  if (useAsRanges) {
	    if (unsortedPossibleInt16) {
	      std::sort(sortedIndicesInt16.begin(), sortedIndicesInt16.end());
	      unsortedPossibleInt16 = false;
	    }
	    T range = 16;
	    sortedIndicesInt16 = TableManager::mergeToRange(std::move(sortedIndicesInt16), range);
	  }
	} else {
	  if (useAsRanges) {
	    T range = 16;
	    TableManager::mergeSortedFlatPairsToRange(sortedIndicesInt16, range);
	  } else {
	    removeDuplicatesWithBitset(sortedIndicesInt16);
	  }
	}
	return &sortedIndicesInt16;
      } else if constexpr(std::is_same_v<LazyT, int8_t>) {
	if (sortedIndicesInt8.size() > 0) {
	  return &sortedIndicesInt8;
	}
	bool fromBase = true;
	bool fromInt32 = false;
	bool fromInt16 = false;
	std::vector<T>* indicesBase = &indices;
	if (sortedIndicesInt16.size() > 0) {
	  indicesBase = &sortedIndicesInt16;
	  fromInt16 = true;
	  fromBase = false;
	} else if (sortedIndicesInt32.size() > 0) {
	  indicesBase = &sortedIndicesInt32;
	  fromInt32 = true;
	  fromBase = false;
	} else if (sortedIndicesInt64.size() > 0) {
	  indicesBase = &sortedIndicesInt64;
	  fromBase = false;
	}

	constexpr int32_t divisor = sizeof(Argument) / sizeof(int8_t);
	constexpr uint32_t shiftAmt = log2_exact(divisor);
	constexpr uint32_t halfShiftAmt = shiftAmt / 2;
	constexpr uint32_t quarterShiftAmt = halfShiftAmt / 2;
	
	sortedIndicesInt8.resize(indicesBase->size());
	const size_t int8N = sortedIndicesInt8.size();
	const auto &indicesBaseVec = *indicesBase;
	if (fromInt16) {
	  if (int8N > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> quarterShiftAmt;
	    }
	  } else if (int8N > SIMD_THRESHOLD) {
#pragma omp simd
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> quarterShiftAmt;
	    }
	  } else {
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> quarterShiftAmt;
	    }
	  }
	} else if (fromInt32) {
	  if (int8N > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> halfShiftAmt;
	    }
	  } else if (int8N > SIMD_THRESHOLD) {
#pragma omp simd
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> halfShiftAmt;
	    }
	  } else {
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> halfShiftAmt;
	    }
	  }
	} else {
	  if (int8N > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> shiftAmt;
	    }
	  } else if (int8N > SIMD_THRESHOLD) {
#pragma omp simd
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> shiftAmt;
	    }
	  } else {
	    for (size_t i = 0; i < int8N; i++) {
	      sortedIndicesInt8[i] = indicesBaseVec[i] >> shiftAmt;
	    }
	  }
	}
	
	if (fromBase) {
	  removeDuplicatesWithBitset(sortedIndicesInt8);
	  if (useAsRanges) {
	    if (unsortedPossibleInt8) {
	      std::sort(sortedIndicesInt8.begin(), sortedIndicesInt8.end());
	      unsortedPossibleInt8 = false;
	    }
	    T range = 8;
	    sortedIndicesInt8 = TableManager::mergeToRange(std::move(sortedIndicesInt8), range);
	  }
	} else {
	  if (useAsRanges) {
	    T range = 8;
	    TableManager::mergeSortedFlatPairsToRange(sortedIndicesInt8, range);
	  } else {
	    removeDuplicatesWithBitset(sortedIndicesInt8);
	  }
	}
	return &sortedIndicesInt8;
      } else {
	return &indices;
      }
    }

    std::vector<T>* lazilyGetDeduplicatedIndices(ArgumentType type) {
      if (type == ArgumentType::ARGUMENT_TYPE_CHAR) {
	return lazilyGetDeduplicatedIndices<int8_t>();
      } else if (type == ArgumentType::ARGUMENT_TYPE_SHORT) {
	return lazilyGetDeduplicatedIndices<int16_t>();
      } else if (type == ArgumentType::ARGUMENT_TYPE_INT) {
	return lazilyGetDeduplicatedIndices<int32_t>();
      } else {
	return lazilyGetDeduplicatedIndices<int64_t>();
      }
    }

    ExpressionSpanArguments getBitPackedConvertedIndicesAsExpressionSpanArguments(int32_t bitsPerVal, ArgumentType packedType) {
      if (isRanged) {
	throw std::runtime_error("Attempt to call getBitPackedConverted on Ranged IndicesManager. Operation is not possible.");
      }
      ExpressionSpanArguments args;
      if (!isSet) {
	return std::move(args);
      }
      const int32_t bitsPerUnit = getArgSizeFromType(packedType) * 8;
      const int32_t valuesPerUnit = bitsPerUnit / bitsPerVal;

      bool isPowerOfTwo = (valuesPerUnit != 0) && ((valuesPerUnit & (valuesPerUnit - 1)) == 0);
      const int32_t shiftAmt = isPowerOfTwo ? __builtin_ctz(valuesPerUnit) : 0;
      
      const size_t indicesN = indices.size();
      std::vector<T> bitPackedIndices(indicesN);
      if (isPowerOfTwo) {
	if (indicesN > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	  for (size_t i = 0; i < indicesN; i++) {
	    bitPackedIndices[i] = indices[i] >> shiftAmt;
	  }
	} else if (indicesN > SIMD_THRESHOLD) {
#pragma omp simd
	  for (size_t i = 0; i < indicesN; i++) {
	    bitPackedIndices[i] = indices[i] >> shiftAmt;
	  }
	} else {
	  for (size_t i = 0; i < indicesN; i++) {
	    bitPackedIndices[i] = indices[i] >> shiftAmt;
	  }
	}
      } else {
	if (indicesN > THREADING_THRESHOLD) {
#pragma omp parallel for simd num_threads(4)
	  for (size_t i = 0; i < indicesN; i++) {
	    bitPackedIndices[i] = indices[i] / valuesPerUnit;
	  }
	} else if (indicesN > SIMD_THRESHOLD) {
#pragma omp simd
	  for (size_t i = 0; i < indicesN; i++) {
	    bitPackedIndices[i] = indices[i] / valuesPerUnit;
	  }
	} else {
	  for (size_t i = 0; i < indicesN; i++) {
	    bitPackedIndices[i] = indices[i] / valuesPerUnit;
	  }
	}
      }
      args.push_back(std::move(boss::Span<T>(std::move(bitPackedIndices))));
      return args;
    }

    // NOT SUPPORTED FOR RANGED INDICES YET
    // DOES NOT SUPPORT NON INT64 INDICES
    ExpressionSpanArguments getFORConvertedIndicesAsExpressionSpanArguments(int32_t frameSize, int64_t spanSize, TableManager& tableMan) {
      if (isRanged) {
	throw std::runtime_error("Attempt to call getFORConverted on Ranged IndicesManager. Operation is not possible.");
      }
      ExpressionSpanArguments args;
      if (!isSet) {
	return std::move(args);
      }
      std::vector<T> forIndices;
      if (spanSize > frameSize) {
	robin_hood::unordered_set<T> forSet;
	auto numRefs = (spanSize + frameSize - 1) / frameSize;
	forIndices.reserve(numRefs);
	if (sortedIndicesInt64FOR.size() == 0) {
	  sortedIndicesInt64FOR.resize(indices.size());
	  std::copy(indices.begin(), indices.end(), sortedIndicesInt64FOR.begin());
	  if (duplicatesPossible) {
	    removeDuplicatesWithBitset(sortedIndicesInt64);
	  }
	}
	for (auto i = 0; i < sortedIndicesInt64FOR.size(); i++) {
	  T refI = static_cast<T>(sortedIndicesInt64FOR[i] / frameSize);
	  if (auto it = forSet.find(refI); it == forSet.end()) {
	    forIndices.push_back(refI);
	    forSet.insert(refI);
	  }
	}
      } else {
	if (spanSize > 0) {
	  forIndices.push_back(0);
	}
      }
      args.emplace_back(boss::Span<T>(std::move(forIndices)));
      return std::move(args);
    }

    ExpressionSpanArguments getIndicesAsExpressionSpanArguments() {      
      ExpressionSpanArguments args;
      if (!isSet) {
	throw std::runtime_error("Attempt to call getIndicesAsExpressionSpanArguments on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	std::vector<T> fullIndices;
	fullIndices.reserve(numIndices);
	for (auto i = 0; i < indices.size(); i += 2) {
	  auto &start = indices[i];
	  auto &size = indices[i + 1];
	  for (auto j = 0; j < size; j++) {
	    fullIndices.emplace_back(start + j);
	  }
	}
	args.emplace_back(boss::Span<T>(std::move(fullIndices)));
      } else {
	args.emplace_back(boss::Span<T>(std::move(indices)));
      }
      return std::move(args);
    }

    std::vector<std::pair<T, T>> getMinimisedIntervals(int32_t numRanges, ArgumentType type, TableManager& tableMan) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getMinimisedIntervals on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	throw std::runtime_error("Attempt to produce minimised intervals for ranged indices. Operation is not possible.");
      }
      auto minIntervalsTypeIt = minimisedIntervalsMap.find(type);
      if (minIntervalsTypeIt == minimisedIntervalsMap.end()) {
	std::unordered_map<int32_t, std::vector<std::pair<T, T>>> innerMap;
	minimisedIntervalsMap.emplace(type, std::move(innerMap));
      }
      auto& innerMinIntervalsMap = minimisedIntervalsMap[type];
      auto minIntervalsIt = innerMinIntervalsMap.find(numRanges);
      if (minIntervalsIt == innerMinIntervalsMap.end()) {
	size_t argSize = getArgSizeFromType(type);
	size_t valsPerArg = sizeof(Argument) / argSize;
	std::vector<T>* argSizeAdjustedIndices = lazilyGetDeduplicatedIndices(type);
        auto [intervals, nonAdjacent] = createAdjacentIntervalsAndNonAdjacents(*argSizeAdjustedIndices);
	if (intervals.size() < numRanges) {
	  auto minNonAdjIntervals = processIntervalsAsync(nonAdjacent, numRanges - intervals.size(), NUM_THREADS);
	  if (intervals.size() > minNonAdjIntervals.size()) {
	    intervals.reserve(intervals.size() + minNonAdjIntervals.size());
	    intervals.insert(intervals.end(),
			     std::make_move_iterator(minNonAdjIntervals.begin()),
			     std::make_move_iterator(minNonAdjIntervals.end()));
	    innerMinIntervalsMap.emplace(numRanges, std::move(intervals));
	  } else {
	    minNonAdjIntervals.reserve(minNonAdjIntervals.size() + intervals.size());
	    minNonAdjIntervals.insert(minNonAdjIntervals.end(),
			     std::make_move_iterator(intervals.begin()),
			     std::make_move_iterator(intervals.end()));
	    innerMinIntervalsMap.emplace(numRanges, std::move(minNonAdjIntervals));
	  }
	} else {
	  auto minIntervals = processIntervalsAsync(*argSizeAdjustedIndices, numRanges, NUM_THREADS);
	  innerMinIntervalsMap.emplace(numRanges, std::move(minIntervals));
	}
      }
      auto &minIntervals = minimisedIntervalsMap[type][numRanges];
      return minIntervals;
    }

    size_t getArgSizeFromType(ArgumentType type) {
      switch(type) {
      case ArgumentType::ARGUMENT_TYPE_BOOL:
        return sizeof(bool);
      case ArgumentType::ARGUMENT_TYPE_CHAR:
        return sizeof(int8_t);
      case ArgumentType::ARGUMENT_TYPE_SHORT:
        return sizeof(int16_t);
      case ArgumentType::ARGUMENT_TYPE_INT:
        return sizeof(int32_t);
      case ArgumentType::ARGUMENT_TYPE_LONG:
        return sizeof(int64_t);
      case ArgumentType::ARGUMENT_TYPE_FLOAT:
        return sizeof(float);
      case ArgumentType::ARGUMENT_TYPE_DOUBLE:
        return sizeof(double);
      case ArgumentType::ARGUMENT_TYPE_STRING:
        return sizeof(int64_t);
      case ArgumentType::ARGUMENT_TYPE_SYMBOL:
        return sizeof(int64_t);
      case ArgumentType::ARGUMENT_TYPE_EXPRESSION:
        return sizeof(int64_t);
      }
    }

    std::vector<size_t> getArgOffsets(size_t startChildOffset, size_t numChildren, ArgumentType type, TableManager& tableMan) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getArgOffsets on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	throw std::runtime_error("Attempt to call getArgOffsets for ranged indices. Operation is not possible. Try getArgOffsetsRanged.");
      }
      std::vector<T>* argSizeAdjustedIndices = lazilyGetDeduplicatedIndices(type);
      std::vector<size_t> resOffsets(argSizeAdjustedIndices->size());
      for (size_t i = 0; i < argSizeAdjustedIndices->size(); i++) {
	auto &index = (*argSizeAdjustedIndices)[i];
	resOffsets[i] = startChildOffset + (index * (index >= 0 && index <= numChildren));
      }
      return std::move(resOffsets);
    }
    
    std::vector<std::pair<size_t, size_t>> getMinArgIntervals(size_t startChildOffset, int32_t numRanges, ArgumentType type, TableManager& tableMan) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getMinArgIntervals on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	throw std::runtime_error("Attempt to call getArgOffsets for ranged indices. Operation is not possible. Try getArgOffsetsRanged.");
      }
      std::vector<std::pair<size_t, size_t>> minimisedArgIntervals;
      const auto &minIntervals = getMinimisedIntervals(numRanges, type, tableMan);
      minimisedArgIntervals.reserve(minIntervals.size());

      std::stringstream intervals;
      for (const auto &[start, end] : minIntervals) {
	minimisedArgIntervals.emplace_back(start + startChildOffset, end + startChildOffset);
      }
      if (duplicatesPossible) {
	TableManager::removeDuplicates(minimisedArgIntervals);
      }
      return std::move(minimisedArgIntervals);
    }

    std::pair<std::vector<size_t>, std::vector<size_t>> getArgOffsetsRanged(size_t startChildOffset) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getArgOffsetsRanged on Unset IndicesManager. Operation is not possible.");
      }
      if (!isRanged) {
	throw std::runtime_error("Attempt to call getArgOffsetsRanged for non-ranged indices. Operation is not possible. Try getArgOffets or getMinArgIntervals.");
      }
      std::vector<size_t> startOffsets;
      std::vector<size_t> sizes;
      startOffsets.reserve(indices.size() / 2);
      sizes.reserve(indices.size() / 2);
      for (auto i = 0; i < indices.size(); i += 2) {
	auto &start = indices[i];
	auto &size = indices[i + 1];
	startOffsets.emplace_back(start + startChildOffset);
	sizes.emplace_back(size);
      }
      auto res = std::make_pair(startOffsets, sizes);
      return std::move(res);
    }

    std::vector<size_t> getStringOffsets(LazilyDeserializedExpression &lazyExpr, size_t numChildren, bool isSpan, TableManager& tableMan) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getStringOffsets on Unset IndicesManager. Operation is not possible.");
      }
      
      std::vector<size_t> resOffsets;
      if (isRanged) {
        resOffsets.reserve(numIndices);
	for (auto i = 0; i < indices.size(); i += 2) {
	  auto &start = indices[i];
	  auto &size = indices[i + 1];
	  for (auto j = 0; j < size; j++) {
	    auto lazyChildExpr = lazyExpr[start + j];
	    resOffsets.emplace_back(lazyChildExpr.getCurrentExpressionAsString(isSpan));
	  }
	}
      } else {
	auto sortedIndicesInt64Ptr = lazilyGetDeduplicatedIndices<int64_t>();
	if (useAsRanges) {
	  resOffsets.reserve(numIndices);
	  for (size_t i = 0; i < sortedIndicesInt64.size(); i += 2) {
	    for (size_t start = sortedIndicesInt64[i]; start <= sortedIndicesInt64[i+1]; start++) {
	      auto lazyChildExpr = lazyExpr[start];
	      resOffsets.emplace_back(lazyChildExpr.getCurrentExpressionAsString(isSpan));
	    }
	  }
	} else {
	  return lazyExpr.getCurrentExpressionAsStringOffsetsVectorWithIndices(sortedIndicesInt64);
	}
      }
      return std::move(resOffsets);
    }

    IndicesManager(IndicesManager const&) = delete;
    IndicesManager &operator=(IndicesManager const&) = delete;
    
    IndicesManager(IndicesManager &&other) noexcept
      : isRanged(other.isRanged), numIndices(other.numIndices), duplicatesPossible(other.duplicatesPossible)
    {
      indices = std::move(other.indices);
      sortedIndicesInt64FOR = std::move(other.sortedIndicesInt64FOR);
      sortedIndicesInt64 = std::move(other.sortedIndicesInt64);
      sortedIndicesInt32 = std::move(other.sortedIndicesInt32);
      sortedIndicesInt16 = std::move(other.sortedIndicesInt16);
      sortedIndicesInt8 = std::move(other.sortedIndicesInt8);
      minimisedIntervalsMap = std::move(other.minimisedIntervalsMap);
      name = std::move(other.name);
    };
    IndicesManager &operator=(IndicesManager &&other) noexcept {
      if (this != &other) {
        isRanged = other.isRanged;
	duplicatesPossible = other.duplicatesPossible;
        numIndices = other.numIndices;
	indices = std::move(other.indices);
	sortedIndicesInt64FOR = std::move(other.sortedIndicesInt64FOR);
	sortedIndicesInt64 = std::move(other.sortedIndicesInt64);
	sortedIndicesInt32 = std::move(other.sortedIndicesInt32);
	sortedIndicesInt16 = std::move(other.sortedIndicesInt16);
	sortedIndicesInt8 = std::move(other.sortedIndicesInt8);
	minimisedIntervalsMap = std::move(other.minimisedIntervalsMap);
	name = std::move(other.name);
      }
      return *this;
    };
    IndicesManager() = default;
    ~IndicesManager() = default;
  };
  
  boss::Expression
  orderSelectionsByRemoteSelectivity(ComplexExpression &&expr,
				     std::unordered_map<std::string, TableManager> &tableMap);
  
  ArgumentType getTypeOfColumn(LazilyDeserializedExpression &lazyExpr);
  
  template<typename T>
  boss::expressions::ExpressionSpanArguments
  getSpanFromIndices(LazilyDeserializedExpression &lazyExpr,
		     TableManager &tableMan,
                     IndicesManager<T> &indices,
                     const std::string &url, EvalFunction &loader, bool isSpan,
		     int64_t spanSize, int64_t numSpansOut, int64_t numThreads,
		     boss::Symbol currColumn) requires IndicesInt<T>;

  template<typename T>
  boss::expressions::ExpressionSpanArgument getSpanFromIndexRanges(
      LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
      IndicesManager<T> &indexRanges,
      const std::string &url, EvalFunction &loader, bool isSpan) requires IndicesInt<T>;


  template<typename T>
  boss::Expression
  decompressFOR(boss::ComplexExpression&& refsExpr, IndicesManager<T>& refsIndicesMan,
		boss::ComplexExpression&& diffsExpr, IndicesManager<T>& diffsIndicesMan,
		int32_t frameSize) requires IndicesInt<T>;

  template<typename T>
  ExpressionSpanArguments
  deriveRLEValueIndicesFromRLEStarts(LazilyDeserializedExpression &lazyExpr,
				     TableManager &tableMan,
				     IndicesManager<T> &indicesMan,
				     const std::string &url,
				     EvalFunction &loader) requires IndicesInt<T>;
    
  template<typename T>
  boss::Expression
  lazyGather(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
             IndicesManager<T> &indices,
             const std::vector<boss::Symbol> &columns, const std::string &url,
             EvalFunction &loader, int64_t spanSizeOut, int64_t numSpansOut,
	     boss::Symbol currColumn, int64_t numThreads) requires IndicesInt<T>;

  template<typename T>
  boss::Expression
  lazyGatherDictionaryEncodedList(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
				  IndicesManager<T> &indices,
				  const std::vector<boss::Symbol> &columns, const std::string &url,
				  EvalFunction &loader, int64_t spanSizeOut, int64_t numSpansOut,
				  boss::Symbol currColumn, int64_t numThreads) requires IndicesInt<T>;
  template<typename T>
  boss::Expression
  lazyGatherBitPackedList(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
				  IndicesManager<T> &indices,
				  const std::vector<boss::Symbol> &columns, const std::string &url,
				  EvalFunction &loader) requires IndicesInt<T>;
  template<typename T>
  boss::Expression
  lazyGatherSingleValueList(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
			    IndicesManager<T> &indices,
			    const std::vector<boss::Symbol> &columns, const std::string &url,
			    EvalFunction &loader, int64_t spanSizeOut, int64_t numSpansOut,
			    int64_t numThreads) requires IndicesInt<T>;
  template<typename T>
  boss::Expression
  lazyGatherSequenceEncodedList(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
				IndicesManager<T> &indices,
				const std::vector<boss::Symbol> &columns, const std::string &url,
				EvalFunction &loader, int64_t spanSizeOut, int64_t numSpansOut,
				int64_t numThreads) requires IndicesInt<T>;

  template<typename T>
  boss::Expression
  lazyGatherFrameOfReferenceList(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
				  IndicesManager<T> &indices,
				  const std::vector<boss::Symbol> &columns, const std::string &url,
				  EvalFunction &loader) requires IndicesInt<T>;

  template<typename T>
  boss::Expression
  lazyGatherRunLengthEncodedList(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
				  IndicesManager<T> &indices,
				  const std::vector<boss::Symbol> &columns, const std::string &url,
				  EvalFunction &loader) requires IndicesInt<T>;

  boss::ComplexExpression
  gatherTableNoIndices(const std::string &url, EvalFunction &loader,
		       ExpressionSpanArguments &&indices,
		       const std::vector<boss::Symbol> &columns);

  boss::Expression
  gatherAndMerge(ExpressionArguments &urls, EvalFunction &loader,
		 const std::vector<boss::Symbol> &columns);

  template <typename T, IndicesInt U>
  static boss::expressions::ExpressionSpanArguments
  getSpan(ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
          IndicesManager<U> &indicesMan,
          size_t numChildren, int64_t spanSize,
	  int64_t numSpansOut, int64_t numThreads) {
    assert(sizeof(T) > 0);
    if (spanSize <= 0 && numSpansOut <= 0) {
      boss::expressions::ExpressionSpanArguments res;
      res.push_back(std::move(lazyExpr.template getCurrentExpressionAsSpanWithIndices<T, U>(indicesMan.indices)));
      return res;
    } else {
      return lazyExpr.template getCurrentExpressionAsSpanWithIndices<T, U>(indicesMan.indices, spanSize, numSpansOut, numThreads);
    }
  }

  template <typename T, IndicesInt U>
  static boss::expressions::ExpressionSpanArgument
  getSpanFromRange(ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
                   IndicesManager<U> &indicesMan) {
    assert(sizeof(T) > 0);
    auto numIndices = indicesMan.numIndices;
    auto &indices = indicesMan.indices;
    size_t valsPerArg = sizeof(Argument) / sizeof(T);
    std::vector<T> matches(numIndices);
    for (size_t i = 0, matchI = 0; i < indices.size(); i += 2) {
      auto &start = indices[i];
      auto &size = indices[i + 1];
      for (auto j = 0; j < size; j++, matchI++) {
	auto lazyChildExpr = lazyExpr((start + j)/ valsPerArg, start + j);
	matches[matchI] = lazyChildExpr.template getCurrentExpressionInSpanAtAs<T>(start + j);
      }
    }
    return boss::Span<T>(std::move(matches));
  }

  int64_t maxRanges;

  std::unordered_map<
      ArgumentType,
      std::function<boss::expressions::ExpressionSpanArguments(
	  ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
          IndicesManager<int32_t> &indices, size_t numChildren, int64_t spanSize, int64_t numSpansOut, int64_t numThreads)>>
      spanFunctors = {
    {ArgumentType::ARGUMENT_TYPE_BOOL, getSpan<bool, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_CHAR, getSpan<int8_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_SHORT, getSpan<int16_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_INT, getSpan<int32_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_LONG, getSpan<int64_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_FLOAT, getSpan<float_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_DOUBLE, getSpan<double_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_STRING, getSpan<std::string, int32_t>}};

  std::unordered_map<
      ArgumentType,
      std::function<boss::expressions::ExpressionSpanArgument(
							      ArgumentType argType,
							      LazilyDeserializedExpression &lazyExpr,
							      IndicesManager<int32_t> &indices)>>
      spanFromRangeFunctors = {
          {ArgumentType::ARGUMENT_TYPE_BOOL, getSpanFromRange<bool, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_CHAR, getSpanFromRange<int8_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_SHORT, getSpanFromRange<int16_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_INT, getSpanFromRange<int32_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_LONG, getSpanFromRange<int64_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_FLOAT, getSpanFromRange<float_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_DOUBLE, getSpanFromRange<double_t, int32_t>},
          {ArgumentType::ARGUMENT_TYPE_STRING, getSpanFromRange<std::string, int32_t>}};

  std::unordered_map<
      ArgumentType,
      std::function<boss::expressions::ExpressionSpanArguments(
	  ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
          IndicesManager<int64_t> &indices, size_t numChildren, int64_t spanSize, int64_t numSpansOut, int64_t numThreads)>>
      spanFunctors64 = {
    {ArgumentType::ARGUMENT_TYPE_BOOL, getSpan<bool, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_CHAR, getSpan<int8_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_SHORT, getSpan<int16_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_INT, getSpan<int32_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_LONG, getSpan<int64_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_FLOAT, getSpan<float_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_DOUBLE, getSpan<double_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_STRING, getSpan<std::string, int64_t>}};

  std::unordered_map<
      ArgumentType,
      std::function<boss::expressions::ExpressionSpanArgument(
							      ArgumentType argType,
							      LazilyDeserializedExpression &lazyExpr,
							      IndicesManager<int64_t> &indices)>>
      spanFromRangeFunctors64 = {
          {ArgumentType::ARGUMENT_TYPE_BOOL, getSpanFromRange<bool, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_CHAR, getSpanFromRange<int8_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_SHORT, getSpanFromRange<int16_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_INT, getSpanFromRange<int32_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_LONG, getSpanFromRange<int64_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_FLOAT, getSpanFromRange<float_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_DOUBLE, getSpanFromRange<double_t, int64_t>},
          {ArgumentType::ARGUMENT_TYPE_STRING, getSpanFromRange<std::string, int64_t>}};

  std::unordered_map<std::string, TableManager> tableMap;

  bool duplicateIndicesPossible = false;
  bool unsortedIndicesPossible = false;
  bool newIndicesPossible = false;

  int64_t paramValsPerBit = 8;
  int64_t paramNumSpansOut = -1;
  int64_t paramNumThreads = -1;
};

  thread_local std::unordered_set<boss::Symbol> Engine::TableManager::columnsLoaded; 

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::WisentDeserialiser

#endif
