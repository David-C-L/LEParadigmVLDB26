#include "ThreadSafeQueue.hpp"
#include "MultipartRangesResponseParser.hpp"

#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <string_view>
#include <charconv>
#include <curl/curl.h>
#include <iostream>
#include <set>
#include <utility>
#include <memory>
#include <future>
#include <thread>
#include <unordered_set>

// #define HEADERDEBUG

namespace boss::engines::RBL {

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() {
    for (auto &[url, curlHandle] : curlHandlesMap) {
      curl_easy_cleanup(curlHandle);
    }

    if (curlGlobalSet) {
      curl_global_cleanup();
    }
  };

  boss::Expression evaluate(boss::Expression &&e);

  struct WriteBuffer {
    std::vector<char> data;
    size_t offset;
    char errorBuffer[CURL_ERROR_SIZE];
  };

  struct WriteBufferDirect {
    int8_t *buffer;
    size_t offset;
  };

  struct BufferManager {

    int8_t *buffer;
    bool exists;
    size_t totalSize;
    size_t currentOffset;

    int64_t totalLoaded;
    int64_t totalDownloaded;
    int64_t totalRequested;
    int64_t totalHeadersDownloaded;
    int64_t totalPretransferTime;
    int64_t totalDownloadTime;

    std::map<int64_t, int64_t> freeBoundsMap;

    using Range = std::pair<int64_t, int64_t>;
    std::vector<int64_t> populatedRanges;

    bool rangesOverlapOrTouch(int64_t start1, int64_t end1, int64_t start2, int64_t end2) {
      return !(end1 < start2 || end2 < start1);
    }

    void mergeRanges(int64_t& start1, int64_t& end1, int64_t start2, int64_t end2) {
      start1 = std::min(start1, start2);
      end1 = std::max(end1, end2);
    }

    std::vector<Range> findGaps(int64_t newStart, int64_t newEnd) {
      std::vector<Range> gaps;
      int64_t currStart = newStart;

      for (size_t rangeI = 0; rangeI < populatedRanges.size(); rangeI += 2) {
	int64_t rangeStart = populatedRanges[rangeI];
	int64_t rangeEnd = populatedRanges[rangeI + 1];

	if (rangeEnd <= newStart) {
	  continue;
	}
	if (rangeStart >= newEnd) {
	  break;
	}
	if (rangeStart > currStart) {
	  gaps.emplace_back(currStart, std::min(newEnd, rangeStart));
	}

	currStart = std::max(currStart, rangeEnd);
	if (currStart >= newEnd) {
	  break;
	}
      }

      if (currStart < newEnd) {
	gaps.emplace_back(currStart, newEnd);
      }

      return std::move(gaps);
    }

    std::vector<Range> addRange(int64_t lb, int64_t ub) {
      Range newRange = {lb, ub};
      std::vector<Range> gaps = findGaps(lb, ub);


      for (size_t rangeI = 0; rangeI < populatedRanges.size(); rangeI += 2) {
	int64_t rangeStart = populatedRanges[rangeI];
	int64_t rangeEnd = populatedRanges[rangeI + 1];
	
	if (rangesOverlapOrTouch(rangeStart, rangeEnd, lb, ub)) {
	  mergeRanges(lb, ub, rangeStart, rangeEnd);
	  populatedRanges.erase(populatedRanges.begin() + rangeI, populatedRanges.begin() + rangeI + 2);
	  rangeI -= 2;
	}
      }
      
      populatedRanges.push_back(lb);
      populatedRanges.push_back(ub);
      std::sort(populatedRanges.begin(), populatedRanges.end());

      return std::move(gaps);
    }

    std::vector<std::pair<int64_t, int64_t>> requestBounds(int64_t lb,
                                                           int64_t ub) {
      if (ub > totalSize) {
	ub = totalSize;
      }
      if (lb >= ub) {
	return {};
      }
      std::vector<std::pair<int64_t, int64_t>> toLoad;
      std::vector<std::pair<int64_t, int64_t>> toAdd;
      for (auto it = freeBoundsMap.begin(); it != freeBoundsMap.end();) {
        auto const &freeLB = it->first;
        auto const &freeUB = it->second;
        if (freeLB <= lb && ub <= freeUB) {
          if (lb != ub) {
            toLoad.emplace_back(lb, ub);
          }
          if (ub < freeUB)
            freeBoundsMap[ub] = freeUB;
          if (freeLB < lb) {
            freeBoundsMap[freeLB] = lb;
          } else {
            it = freeBoundsMap.erase(it);
          }
          return toLoad;
        }
        if (freeLB <= lb && freeUB <= ub && lb < freeUB) {
          toLoad.emplace_back(lb, freeUB);
          toAdd.emplace_back(freeLB, lb);
          it = freeBoundsMap.erase(it);
        } else if (lb <= freeLB && ub <= freeUB && freeLB < ub) {
          toLoad.emplace_back(freeLB, ub);
          toAdd.emplace_back(ub, freeUB);
          it = freeBoundsMap.erase(it);
        } else if (lb <= freeLB && freeUB <= ub) {
          if (freeLB != freeUB) {
            toLoad.emplace_back(freeLB, freeUB);
          }
          it = freeBoundsMap.erase(it);
        } else {
          ++it;
        }
      }

      for (const auto &[newLB, newUB] : toAdd) {
        freeBoundsMap[newLB] = newUB;
      }

      return toLoad;
    }

    void addLoaded(std::vector<std::pair<int64_t, int64_t>> &bounds) {
      for (const auto &[lb, ub] : bounds) {
        totalLoaded += ub - lb;
      }
    }

    explicit BufferManager(size_t totalSize, bool exists)
      : exists(exists), totalSize(totalSize), currentOffset(0), totalLoaded(0), totalDownloaded(0),
          totalRequested(0), totalHeadersDownloaded(0), totalPretransferTime(0),
          totalDownloadTime(0), freeBoundsMap{{0, totalSize}} {
      if (exists) {
	buffer = (int8_t *)std::malloc(totalSize);
      } else {
	buffer = nullptr;
      }
    }

    BufferManager(const BufferManager &other)
      : exists(other.exists), totalSize(other.totalSize), currentOffset(other.currentOffset),
          totalLoaded(other.totalLoaded), totalDownloaded(other.totalDownloaded), totalRequested(other.totalRequested),
          totalHeadersDownloaded(other.totalHeadersDownloaded),
          totalPretransferTime(other.totalPretransferTime),
          totalDownloadTime(other.totalDownloadTime),
          freeBoundsMap(other.freeBoundsMap) {
      if (exists) {
	buffer = (int8_t *)std::malloc(totalSize);
	std::memcpy(buffer, other.buffer, totalSize);
      }
    };
    BufferManager &operator=(const BufferManager &other) {
      if (this != &other) {
        std::free(buffer);
	exists = other.exists;
        totalSize = other.totalSize;
        currentOffset = other.currentOffset;
        totalLoaded = other.totalLoaded;
        totalDownloaded = other.totalDownloaded;
        totalRequested = other.totalRequested;
        totalHeadersDownloaded = other.totalHeadersDownloaded;
        totalPretransferTime = other.totalPretransferTime;
        totalDownloadTime = other.totalDownloadTime;
	if (exists) {
	  buffer = (int8_t *)std::malloc(totalSize);
	  std::memcpy(buffer, other.buffer, totalSize);
	}
        freeBoundsMap = other.freeBoundsMap;
      }
      return *this;
    };
    BufferManager(BufferManager &&other) noexcept
      : buffer(other.buffer), exists(other.exists), totalSize(other.totalSize),
          currentOffset(other.currentOffset), totalLoaded(other.totalLoaded), totalDownloaded(other.totalDownloaded),
          totalRequested(other.totalRequested),
          totalHeadersDownloaded(other.totalHeadersDownloaded),
          totalPretransferTime(other.totalPretransferTime),
          totalDownloadTime(other.totalDownloadTime),
          freeBoundsMap(std::move(other.freeBoundsMap)) {
      other.buffer = nullptr;
      other.exists = false;
      other.totalSize = 0;
      other.currentOffset = 0;
      other.totalLoaded = 0;
      other.totalRequested = 0;
      other.totalHeadersDownloaded = 0;
      other.totalPretransferTime = 0;
      other.totalDownloadTime = 0;
    };
    BufferManager &operator=(BufferManager &&other) noexcept {
      if (this != &other) {
        std::free(buffer);
        buffer = other.buffer;
	exists = other.exists;
        totalSize = other.totalSize;
        currentOffset = other.currentOffset;
        totalLoaded = other.totalLoaded;
        totalDownloaded = other.totalDownloaded;
        totalRequested = other.totalRequested;
        totalHeadersDownloaded = other.totalHeadersDownloaded;
        totalPretransferTime = other.totalPretransferTime;
        totalDownloadTime = other.totalDownloadTime;
        freeBoundsMap = std::move(other.freeBoundsMap);
        other.buffer = nullptr;
	other.exists = false;
        other.totalSize = 0;
        other.currentOffset = 0;
        other.totalLoaded = 0;
        other.totalDownloaded = 0;
        other.totalRequested = 0;
        other.totalHeadersDownloaded = 0;
        other.totalPretransferTime = 0;
      }
      return *this;
    };
    BufferManager() = default;
    ~BufferManager() {
      if (buffer != nullptr)
        std::free(buffer);
    };
  };

  struct DummyBufferManager : private BufferManager {

    int64_t totalRequired;

    std::vector<std::pair<int64_t, int64_t>> requestBounds(int64_t lb,
                                                           int64_t ub) {
      return BufferManager::requestBounds(lb, ub);
    }

    void addLoaded(std::vector<std::pair<int64_t, int64_t>> &bounds) {
      for (const auto &[lb, ub] : bounds) {
        totalRequired += ub - lb;
      }
    }

    explicit DummyBufferManager(size_t totalSize) : totalRequired(0) {
      totalSize = totalSize;
      freeBoundsMap = {{0, totalSize}};
      buffer = (int8_t *)std::malloc(1);
    }

    DummyBufferManager(const DummyBufferManager &other)
        : totalRequired(other.totalRequired) {
      totalSize = other.totalSize;
      buffer = (int8_t *)std::malloc(1);
      freeBoundsMap = other.freeBoundsMap;
    };

    DummyBufferManager &operator=(const DummyBufferManager &other) {
      if (this != &other) {
        totalSize = other.totalSize;
        totalRequired = other.totalRequired;
        freeBoundsMap = other.freeBoundsMap;
      }
      return *this;
    };

    DummyBufferManager(DummyBufferManager &&other) noexcept
        : totalRequired(other.totalRequired) {
      totalSize = other.totalSize;
      buffer = other.buffer;
      freeBoundsMap = std::move(other.freeBoundsMap);
      other.totalSize = 0;
      other.buffer = nullptr;
      other.totalRequired = 0;
    };
    DummyBufferManager &operator=(DummyBufferManager &&other) noexcept {
      if (this != &other) {
        totalSize = other.totalSize;
        totalRequired = other.totalRequired;
        freeBoundsMap = std::move(other.freeBoundsMap);
        other.totalSize = 0;
        other.totalRequired = 0;
      }
      return *this;
    };
    DummyBufferManager() = default;
    ~DummyBufferManager() = default;
  };

  struct MIMEBoundaryHandler {

    enum MIMEBoundaryStage : int32_t { BOUNDARY, BOUNDARY_HANDLED };

    MIMEBoundaryStage currStage = BOUNDARY;
    size_t currBoundaryOffset = 0;
    std::string currBoundary;

    inline size_t findBoundary(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BOUNDARY) {
        return -1;
      }
      const char* boundary = currBoundary.data();
      size_t boundaryLength = currBoundary.length();
      size_t i = currOffset;

      while (i + boundaryLength <= size_b) {
	if (data[i] == boundary[0] &&
	    memcmp(data + i, boundary, boundaryLength) == 0) {
	  currStage = BOUNDARY_HANDLED;
	  currBoundaryOffset = boundaryLength;
	  return i + boundaryLength;
	}
	i++;
      }
      
      while (currBoundaryOffset < boundaryLength && i < size_b) {
        if (data[i] == currBoundary[currBoundaryOffset]) {
          currBoundaryOffset++;
        } else {
          currBoundaryOffset = 0;
        }
        i++;
      }

      if (currBoundaryOffset == boundaryLength) {
        currStage = BOUNDARY_HANDLED;
      }

      return i;
    }

    size_t handleBoundary(char *data, size_t size_b, size_t currOffset) {
#ifdef HEADERDEBUG
      std::cout << "HANDLE BOUNDARY AT: " << currOffset << std::endl;
#endif
      size_t i = currOffset;
      while (currStage != BOUNDARY_HANDLED && i < size_b) {
        switch (currStage) {
        case BOUNDARY:
          i = findBoundary(data, size_b, i);
          break;
        case BOUNDARY_HANDLED:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == BOUNDARY_HANDLED; }

    void resetHandlerStage() {
      currBoundaryOffset = 0;
      currStage = BOUNDARY;
    };

    MIMEBoundaryHandler(const MIMEBoundaryHandler &) = default;
    MIMEBoundaryHandler &operator=(const MIMEBoundaryHandler &) = default;
    MIMEBoundaryHandler(MIMEBoundaryHandler &&) = default;
    MIMEBoundaryHandler &operator=(MIMEBoundaryHandler &&) = default;
    MIMEBoundaryHandler() = default;
    ~MIMEBoundaryHandler() = default;
  };

  struct MultipartHeaderHandler {

    enum MultipartHeaderStage : int32_t {
      BYTE_RANGE_PREAMBLE,
      BYTE_RANGE,
      BYTE_RANGE_POSTAMBLE,
      MULTIPART_HEADERS_HANDLED
    };

    static constexpr char BYTE_RANGE_KEY[] = "Content-Range: bytes ";
    static constexpr size_t BYTE_RANGE_KEY_LEN = sizeof(BYTE_RANGE_KEY) - 1;
    
    static inline std::string const RANGE_SPLIT = "-";
    static inline char const BYTE_RANGE_END = '/';

    MultipartHeaderStage currStage = BYTE_RANGE_PREAMBLE;

    size_t currPreambleOffset = 0;
    int64_t currLB = -1;
    int64_t currUB = -1;

    char currRangeBuf[128];
    size_t currRangeLen = 0;

    std::string postamble;
    size_t currPostambleOffset = 0;
    size_t postambleLength;

    inline int64_t fastAtoi(const char* str, size_t len) {
      int64_t res = 0;
      for (size_t i = 0; i < len; i++) {
	res = res * 10 + (str[i] - '0');
      }
      return res;
    }

    inline void setPostamble(std::string &&postambleValue) {
      postambleLength = postambleValue.length();
      postamble = std::move(postambleValue);
      currPostambleOffset = 0;

#ifdef HEADERDEBUG
      std::cout << "CURR POSTAMBLE: " << postamble << std::endl;
#endif
    }

    inline void setBoundsFromCurrentRange() {
      std::string_view view(currRangeBuf, currRangeLen);
#ifdef HEADERDEBUG
      std::cout << "SET BOUNDS FROM: " << view << std::endl;
#endif
      auto splitPos = view.find(RANGE_SPLIT);
      if (splitPos != std::string::npos) {
	currLB = fastAtoi(view.data(), splitPos);
	currUB = fastAtoi(view.data() + splitPos + 1, view.size() - splitPos - 1);
#ifdef HEADERDEBUG
        std::cout << "FOUND BYTE RANGE WITH (LB, UB): (" << currLB << ", "
                  << currUB << ")" << std::endl;
#endif
      }
    }

    inline size_t findByteRange(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BYTE_RANGE_PREAMBLE) {
        return -1;
      }
      
      constexpr size_t PREAMBLE_LEN = BYTE_RANGE_KEY_LEN;

      if (size_b - currOffset >= PREAMBLE_LEN &&
	  memcmp(data + currOffset, BYTE_RANGE_KEY, PREAMBLE_LEN) == 0) {
	currStage = BYTE_RANGE;
	currPreambleOffset = PREAMBLE_LEN;
        currRangeLen = 0;
	return currOffset + PREAMBLE_LEN;
      }
      
      size_t i = currOffset;

      while (currPreambleOffset < PREAMBLE_LEN && i < size_b) {
        if (data[i] == BYTE_RANGE_KEY[currPreambleOffset]) {
          currPreambleOffset++;
        } else {
          currPreambleOffset = 0;
        }
        i++;
      }

      if (currPreambleOffset == PREAMBLE_LEN) {
        currRangeLen = 0;
	currStage = BYTE_RANGE;
      }

      return i;
    }

    inline size_t readByteRange(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BYTE_RANGE) {
        return -1;
      }
      size_t i = currOffset;

      while (i < size_b && data[i] != BYTE_RANGE_END && currRangeLen < sizeof(currRangeBuf)) {
	currRangeBuf[currRangeLen++] = data[i];
        i++;
      }

      if (i < size_b && data[i] == BYTE_RANGE_END) {
        setBoundsFromCurrentRange();
        currStage = BYTE_RANGE_POSTAMBLE;
      }

      return i;
    }

    inline size_t consumePostamble(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BYTE_RANGE_POSTAMBLE) {
        return -1;
      }
      size_t i = currOffset;

      while (currPostambleOffset != postambleLength && i < size_b) {
        if (data[i] == postamble[currPostambleOffset]) {
          currPostambleOffset++;
        } else {
          currPostambleOffset = 0;
        }
        i++;
      }

      if (currPostambleOffset == postambleLength) {
        currStage = MULTIPART_HEADERS_HANDLED;
      }

      return i;
    }

    size_t handleMultipartHeader(char *data, size_t size_b, size_t currOffset) {
#ifdef HEADERDEBUG
      std::cout << "HANDLE HEADER AT: " << currOffset << std::endl;
#endif
      size_t i = currOffset;
      while (currStage != MULTIPART_HEADERS_HANDLED && i < size_b) {
        switch (currStage) {
        case BYTE_RANGE_PREAMBLE:
          i = findByteRange(data, size_b, i);
          break;
        case BYTE_RANGE:
          i = readByteRange(data, size_b, i);
          break;
        case BYTE_RANGE_POSTAMBLE:
          i = consumePostamble(data, size_b, i);
          break;
        case MULTIPART_HEADERS_HANDLED:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == MULTIPART_HEADERS_HANDLED; }

    void resetHandlerStage() {
      currPreambleOffset = 0;
      currPostambleOffset = 0;
      currLB = -1;
      currUB = -1;
      currRangeLen = 0;
      currStage = BYTE_RANGE_PREAMBLE;
    }

    size_t getMultipartLength() { return (currUB - currLB) + 1; }

    MultipartHeaderHandler(const MultipartHeaderHandler &) = default;
    MultipartHeaderHandler &operator=(const MultipartHeaderHandler &) = default;
    MultipartHeaderHandler(MultipartHeaderHandler &&) = default;
    MultipartHeaderHandler &operator=(MultipartHeaderHandler &&) = default;
    MultipartHeaderHandler() = default;
    ~MultipartHeaderHandler() = default;
  };

  struct MultipartDataHandler {

    enum MultipartDataStage : int32_t {
      MULTIPART_DATA_PREAMBLE,
      MULTIPART_DATA,
      MULTIPART_DATA_POSTAMBLE,
      MULTIPART_DATA_HANDLED
    };

    static inline std::string const DATA_PREAMBLE_STR = "\r\n\r\n";
    static inline std::string const DATA_POSTAMBLE_STR = "\r\n";

    MultipartDataStage currStage = MULTIPART_DATA_PREAMBLE;

    size_t currPreambleOffset = 0;
    size_t preambleLength = DATA_PREAMBLE_STR.length();

    size_t currMultipartDataSize;
    size_t currMultipartDataOffset = 0;

    size_t currPostambleOffset = 0;
    size_t postambleLength = DATA_POSTAMBLE_STR.length();

    void setMultipartData(size_t size) { currMultipartDataSize = size; }

    size_t writeMultipartData(BufferManager *bufferMan, char *data,
                              size_t size_b, size_t currOffset, size_t &currentBufferOffset) {
      if (currStage != MULTIPART_DATA) {
        return -1;
      }

      int64_t multipartUnread = currMultipartDataSize - currMultipartDataOffset;
      int64_t dataUnread = size_b - currOffset;
      int64_t amountToRead;
      if (multipartUnread > dataUnread) {
        amountToRead = dataUnread;
      } else {
        amountToRead = multipartUnread;
        currStage = MULTIPART_DATA_HANDLED;
      }
      memcpy((bufferMan->buffer + currentBufferOffset),
             (data + currOffset), amountToRead);
      currentBufferOffset += amountToRead;
      currMultipartDataOffset += amountToRead;

      return currOffset + amountToRead;
    }

    size_t consumePreamble(char *data, size_t size_b, size_t currOffset) {
      if (currStage != MULTIPART_DATA_PREAMBLE) {
        return -1;
      }
      size_t i = currOffset;

      while (currPreambleOffset != preambleLength && i < size_b) {
        if (data[i] == DATA_PREAMBLE_STR[currPreambleOffset]) {
          currPreambleOffset++;
        } else {
          currPreambleOffset = 0;
        }
        i++;
      }

      if (currPreambleOffset == preambleLength) {
        currStage = MULTIPART_DATA;
      }

      return i;
    }

    size_t consumePostamble(char *data, size_t size_b, size_t currOffset) {
      if (currStage != MULTIPART_DATA_POSTAMBLE) {
        return -1;
      }
      size_t i = currOffset;

      while (currPostambleOffset != postambleLength && i < size_b) {
        if (data[i] == DATA_POSTAMBLE_STR[currPostambleOffset]) {
          currPostambleOffset++;
        } else {
          currPostambleOffset = 0;
        }
        i++;
      }

      if (currPostambleOffset == postambleLength) {
        currStage = MULTIPART_DATA_HANDLED;
      }

      return i;
    }

    size_t handleMultipartData(BufferManager *bufferMan, char *data,
                               size_t size_b, size_t currOffset, size_t &currentBufferOffset) {
#ifdef HEADERDEBUG
      std::cout << "HANDLE DATA AT: " << currOffset << std::endl;
#endif
      size_t i = currOffset;
      while (currStage != MULTIPART_DATA_HANDLED && i < size_b) {
        switch (currStage) {
        case MULTIPART_DATA_PREAMBLE:
          i = consumePreamble(data, size_b, i);
          break;
        case MULTIPART_DATA:
          i = writeMultipartData(bufferMan, data, size_b, i, currentBufferOffset);
          break;
        case MULTIPART_DATA_POSTAMBLE:
          i = consumePostamble(data, size_b, i);
          break;
        case MULTIPART_DATA_HANDLED:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == MULTIPART_DATA_HANDLED; }

    void resetHandlerStage() {
      currPreambleOffset = 0;
      currPostambleOffset = 0;
      currMultipartDataOffset = 0;
      currStage = MULTIPART_DATA_PREAMBLE;
    }

    MultipartDataHandler(const MultipartDataHandler &) = default;
    MultipartDataHandler &operator=(const MultipartDataHandler &) = default;
    MultipartDataHandler(MultipartDataHandler &&) = default;
    MultipartDataHandler &operator=(MultipartDataHandler &&) = default;
    MultipartDataHandler() = default;
    ~MultipartDataHandler() = default;
  };

  struct MultipartResponseHandler {

    enum MultipartResponseStage : int32_t {
      BOUNDARY,
      CHECK_END,
      HEADERS,
      DATA,
      RESET,
      DONE
    };

    static inline std::string const END_STR = "--";
    size_t currEndOffset = 0;
    size_t endLength = END_STR.length();
    bool done = false;

    size_t currentBufferOffset = 0;

    MultipartResponseStage currStage = BOUNDARY;

    MIMEBoundaryHandler boundaryHandler;
    MultipartHeaderHandler headersHandler;
    MultipartDataHandler dataHandler;

    struct curl_slist* attachedHeaderList = nullptr;

    BufferManager *bufferMan;

    size_t isEnd(char *data, size_t size_b, size_t currOffset) {
      if (currStage != CHECK_END) {
        return -1;
      }
      size_t i = currOffset;

      while (currEndOffset != endLength && i < size_b) {
        done = data[i] == END_STR[currEndOffset];
        currEndOffset++;
        i++;
      }

      if (done) {
        currStage = DONE;
      } else {
        currStage = HEADERS;
      }

      return i;
    }

    void resetHandlerStage() { currStage = BOUNDARY; };

    void resetHandlers() {
#ifdef HEADERDEBUG
      std::cout << "HANDLE RESET" << std::endl;
#endif
      boundaryHandler.resetHandlerStage();
      headersHandler.resetHandlerStage();
      dataHandler.resetHandlerStage();
      resetHandlerStage();
    }

    void resetBoundary(std::string &&boundary) {
      boundaryHandler.currBoundary = std::move(boundary);
      boundaryHandler.currBoundaryOffset = 0;
    }

    size_t handleMultipart(char *data, size_t size_b, size_t currOffset) {
      size_t i = currOffset;
#ifdef HEADERDEBUG
      std::cout << "HANDLE MULTIPART WITH SIZE: " << size_b << std::endl;
#endif
      while (currStage != DONE && i < size_b) {
        switch (currStage) {
        case BOUNDARY:
          i = boundaryHandler.handleBoundary(data, size_b, i);
          if (boundaryHandler.isDone()) {
            currStage = CHECK_END;
          }
          break;
        case CHECK_END:
#ifdef HEADERDEBUG
          std::cout << "HANDLE IS_END AT: " << i << std::endl;
#endif
          i = isEnd(data, size_b, i);
          break;
        case HEADERS:
          i = headersHandler.handleMultipartHeader(data, size_b, i);
          if (headersHandler.isDone()) {
            currStage = DATA;
            currentBufferOffset = headersHandler.currLB;
            dataHandler.setMultipartData(headersHandler.getMultipartLength());
          }
          break;
        case DATA:
          i = dataHandler.handleMultipartData(bufferMan, data, size_b, i, currentBufferOffset);
          if (dataHandler.isDone()) {
            currStage = RESET;
          }
          break;
        case RESET:
          resetHandlers();
          break;
        case DONE:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == DONE; }

    MultipartResponseHandler(BufferManager *bufferMan) : bufferMan(bufferMan) {
      headersHandler.setPostamble("*");
    };

    MultipartResponseHandler(const MultipartResponseHandler &) = default;
    MultipartResponseHandler &
    operator=(const MultipartResponseHandler &) = default;
    MultipartResponseHandler(MultipartResponseHandler &&) = default;
    MultipartResponseHandler &operator=(MultipartResponseHandler &&) = default;
    MultipartResponseHandler() = default;
    ~MultipartResponseHandler() = default;
  };
  
  struct URLExistenceInfo {
    bool exists;
    int64_t requested;
    int64_t headers;
    int64_t pretransferTime;
    int64_t downloadTime;
  };

  struct CurlManager {

    size_t maxHandles;
    CURLM* multiHandle;
    std::vector<CURL*> easyHandles;
    std::vector<CURL*> availableHandles;
    std::unordered_set<CURL*> previouslyUsed;

    CurlManager(size_t maxHandles = 32) : maxHandles(maxHandles) {
      multiHandle = curl_multi_init();
      curl_multi_setopt(multiHandle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
    };

    void cleanup() {
      for (auto handle : easyHandles) {
	curl_easy_cleanup(handle);
      }
      easyHandles.clear();
      availableHandles.clear();
      previouslyUsed.clear();

      if (multiHandle) {
	curl_multi_cleanup(multiHandle);
	multiHandle = nullptr;
      }
    }

    CURLM* getMultiHandle() {
      return multiHandle;
    }

    CURL* getEasyHandle() {      
#ifdef HEADERDEBUG
      std::cout << "GETTING: (TOTAL[" << easyHandles.size() << "], AVAILABLE[" << availableHandles.size() << "])" << std::endl;
#endif
      if (!availableHandles.empty()) {
	CURL* handle = availableHandles.back();
	availableHandles.pop_back();
#ifdef HEADERDEBUG
	std::cout << "       AVAILABLE: (TOTAL[" << easyHandles.size() << "], AVAILABLE[" << availableHandles.size() << "])" << std::endl;
#endif
	return handle;
      } else if (easyHandles.size() < maxHandles) {
	CURL* handle = curl_easy_init();
	if (handle) {
	  easyHandles.push_back(handle);
#ifdef HEADERDEBUG
	  std::cout << "      NEW: (TOTAL[" << easyHandles.size() << "], AVAILABLE[" << availableHandles.size() << "])" << std::endl;
#endif
	} else {
	  throw std::runtime_error(
				   "Could not allocate resources for curl handle");
	}
	return handle;
      } else {
	std::cerr << "No available handles and maxHandles reached" << std::endl;
	return nullptr;
      }
    }

    void releaseEasyHandle(CURL* handle) {
      if (handle) {
	availableHandles.push_back(handle);
	previouslyUsed.insert(handle);
     } 
#ifdef HEADERDEBUG
      std::cout << "RELEASING: (TOTAL[" << easyHandles.size() << "], AVAILABLE[" << availableHandles.size() << "])" << std::endl;
#endif
    }

    bool handleUsed(CURL* handle) {
      return previouslyUsed.find(handle) != previouslyUsed.end();
    }
    
    CurlManager(const CurlManager&) = delete;
    CurlManager& operator=(const CurlManager&) = delete;

    CurlManager(CurlManager&& other) noexcept
      : maxHandles(other.maxHandles), multiHandle(other.multiHandle), easyHandles(std::move(other.easyHandles)), availableHandles(std::move(other.availableHandles)), previouslyUsed(std::move(other.previouslyUsed)) {
        other.multiHandle = nullptr;
    }

    CurlManager& operator=(CurlManager&& other) noexcept {
        if (this != &other) {
            cleanup();

            maxHandles = other.maxHandles;
            multiHandle = other.multiHandle;
            easyHandles = std::move(other.easyHandles);
            availableHandles = std::move(other.availableHandles);
            previouslyUsed = std::move(other.previouslyUsed);

            other.multiHandle = nullptr;
        }
        return *this;
    }

    ~CurlManager() {
        cleanup();
    }
  };

  struct RequestsManager {

    struct ParsedTask {
      std::vector<char> data;
      std::string boundary;    
      BufferManager* bufferMan;
    };
    
    size_t currNumThreads;
    size_t currNumHandles;
    
    bool multithreaded;
    int32_t threadRequestType;
    std::string url;
    std::vector<std::vector<std::pair<std::string, size_t>>> currRangeSets;
    URLExistenceInfo existsInfo;

    CURL * mainHandle;
    CurlManager mainManager;
    std::vector<CurlManager> threadCurlManagers;
    std::vector<CURL *> threadCurlHandles;

    std::atomic<bool> stopMonitoring{false};
    std::atomic<bool> stopParsing{false};
    std::atomic<size_t> tasksEnqueued{0};
    std::atomic<size_t> tasksParsed{0};
    ThreadSafeQueue<ParsedTask> parseQueue;
    
    void setupThreadCurlManagers(size_t numThreads, size_t numHandles) {
      threadCurlManagers.clear();
      threadCurlManagers.reserve(numThreads);
      for (auto i = 0; i < numThreads; i++) {
	threadCurlManagers.emplace_back(numHandles);
      }
      for (auto &curlManI : threadCurlManagers) {
	std::vector<CURL*> tmpHandles;
	tmpHandles.reserve(numHandles);
	for (auto i = 0; i < numHandles; i++) {
	  tmpHandles.push_back(curlManI.getEasyHandle());
	}
	for (auto i = 0; i < numHandles; i++) {
	  CURL* easyHandle = tmpHandles[i];
	  if (easyHandle)
	    curlManI.releaseEasyHandle(easyHandle);
	}
      }
    }

    void setupThreadCurlHandles(size_t numThreads) {
      threadCurlHandles.clear();
      threadCurlHandles.reserve(numThreads);
      for (auto i = 0; i < numThreads; i++) {
	CURL *curl;
	curl = curl_easy_init();
	if (curl) {
	  auto existsInfo = checkURLExists(curl, url);
	  threadCurlHandles.push_back(curl);
	} else {
	  throw std::runtime_error(
				   "Could not allocate resources for curl handle");
	}
      }
    }

    void setupMainHandle() {
      mainHandle = curl_easy_init();
      if (mainHandle) {
	existsInfo = checkURLExists(mainHandle, url);
      } else {
	throw std::runtime_error("Could not allocate resources for curl handle");
      }
    }

    void setupMainManager(size_t numHandles) {
      CurlManager tmpMan(numHandles);
      mainManager = std::move(tmpMan);
    }

    RequestsManager(size_t numThreads, size_t numHandles, std::string url, int32_t type = 1)
      : currNumThreads(numThreads),
	currNumHandles(numHandles),
	url(url),
	threadRequestType(type) {
      setupMainHandle();
      setupMainManager(currNumHandles);
      multithreaded = currNumThreads > 1 && threadRequestType != 0;
      if (multithreaded) {
	if (threadRequestType == 1) {
	  setupThreadCurlHandles(currNumThreads);
	} else if (threadRequestType == 2) {
	  setupThreadCurlManagers(currNumThreads, currNumHandles);
	} else if (threadRequestType == 3) {
	  size_t managersThreads = (size_t) std::floor((double_t) currNumThreads / 4.0);
	  size_t handlesThreads = currNumThreads - managersThreads;
	  setupThreadCurlHandles(handlesThreads);
	  setupThreadCurlManagers(managersThreads, currNumHandles);
	}
      }
    }

    void monitoringThreadLoop() {
      using namespace std::chrono;
      auto lastTime = steady_clock::now();

      while (!stopMonitoring) {
	auto now = steady_clock::now();
	auto elapsed = duration_cast<milliseconds>(now - lastTime).count();
	lastTime = now;

	size_t parsed = tasksParsed.load(std::memory_order_relaxed);
	size_t enqueued = tasksEnqueued.load(std::memory_order_relaxed);
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }

    }

    void parserThreadLoop() {
      while (!stopParsing) {
        ParsedTask task;
        if (parseQueue.tryPop(task)) {
	  MultipartRangeParser parser(task.data.data(), task.data.size(),
				      task.bufferMan->buffer, task.bufferMan->totalSize,
				      task.boundary);
	  parser.parseAndApply();
	  tasksParsed.fetch_add(1, std::memory_order_relaxed);
        } else {
	  std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      }
    }
    
    void setUpEasyForMultiAndParseMulti(CURLM *multiCurl, CURL *curl,
					std::string const &url,
					struct curl_slist* headers,
					WriteBuffer* writeData,
					size_t (*writeResponseFunc)(void *, size_t,
								    size_t, void *),
					bool headersAndWrite = false,
					bool addLogs = false) {
      if (curl) {

	if (headersAndWrite) {
	  
	  if (addLogs) {
	    writeData->errorBuffer[0] = '\0';
	    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, writeData->errorBuffer);
	    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
	  }
	  
	  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
          curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	  curl_easy_setopt(curl, CURLOPT_WRITEDATA, writeData);
	  curl_easy_setopt(curl, CURLOPT_PRIVATE, writeData);

	  curl_multi_add_handle(multiCurl, curl);
	  
	  return;
	}
	
	curl_easy_reset(curl);
        
	if (addLogs) {
	  writeData->errorBuffer[0] = '\0';
	  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, writeData->errorBuffer);
	  curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
	}

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
	curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);
	curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 600L);

	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
      	
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, writeData);
        curl_easy_setopt(curl, CURLOPT_PRIVATE, writeData);

	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

	curl_easy_setopt(curl, CURLOPT_UPLOAD_BUFFERSIZE, 64*1024*1024);
	curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 64*1024*1024);
    
	curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);


	curl_multi_add_handle(multiCurl, curl);
      }
    }

    void allocateRangesWithEasyAndParseMulti(BufferManager &bufferMan, CURL* curlHand,
					     std::vector<std::pair<std::string, size_t>> &rangeSets,
					     std::string const &url,
					     size_t (*writeResponseFunc)(void *, size_t,
									 size_t, void *),
					     bool isTrackingOverhead) {
      const size_t totalRequests = rangeSets.size();
      std::vector<std::thread> parserThreads;
      int32_t numParserThreads = 2;
      for (size_t i = 0; i < numParserThreads; i++) {
	parserThreads.emplace_back(&RequestsManager::parserThreadLoop, this);
      }

      bool reset = true;
      for (auto j = 0; j < totalRequests; j++) {
	const auto& [ranges, estimatedSize] = rangeSets[j];
	std::vector<char> responseBuffer(estimatedSize);
	WriteBuffer wb = { std::move(responseBuffer), 0 };

	allocateRangesFromURL(bufferMan, curlHand, ranges, url, nullptr, &wb, writeResponseFunc, isTrackingOverhead, reset);
	reset = false;

	if (wb.offset > 0) {
	  wb.data.resize(wb.offset);
	  ParsedTask task {
	    .data = std::move(wb.data),
	    .boundary = "*",
	    .bufferMan = &bufferMan
	  };
	  parseQueue.push(std::move(task));
	  tasksEnqueued.fetch_add(1, std::memory_order_relaxed);
	}
      }
      
      while (tasksParsed.load() < tasksEnqueued.load()) {
	size_t parsed = tasksParsed.load(std::memory_order_relaxed);
	size_t enqueued = tasksEnqueued.load(std::memory_order_relaxed);
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      stopParsing = true;
      for (auto& thread : parserThreads) {
	thread.join();
      }
      
      ParsedTask dummy;
      while (parseQueue.tryPop(dummy)) {}
      tasksEnqueued = 0;
      tasksParsed = 0;
      stopParsing = false;
      parserThreads.clear();

    }
    
    void allocateRangesWithMultiAndParseMulti(BufferManager &bufferMan, CurlManager &curlMan,
					      std::vector<std::pair<std::string, size_t>> &rangeSets,
					      std::string const &url,
					      size_t (*writeResponseFunc)(void *, size_t,
									  size_t, void *),
					      bool isTrackingOverhead) {
      const size_t totalRequests = rangeSets.size();
      auto maxHandles = curlMan.maxHandles;
#ifdef DEBUG
      std::cout << "TOTAL REQUESTS: " << totalRequests << " MAX HANDLES: " << maxHandles << std::endl;
#endif
      CURLM* multiHandle = curlMan.getMultiHandle();

      std::vector<std::thread> parserThreads;
      int32_t numParserThreads = std::max(2, int32_t(currNumThreads / 2));
      for (size_t i = 0; i < numParserThreads; i++) {
	parserThreads.emplace_back(&RequestsManager::parserThreadLoop, this);
      }

      std::thread monitorThread(&RequestsManager::monitoringThreadLoop, this);

      std::vector<WriteBuffer> writeBuffers;
      std::vector<struct curl_slist*> headers;
      for (auto j = 0; j < totalRequests; j++) {
	const auto& [ranges, estimatedSize] = rangeSets[j];
	std::vector<char> responseBuffer(estimatedSize);
	
	WriteBuffer wb = { std::move(responseBuffer), 0 };
	writeBuffers.push_back(std::move(wb));

	std::string rangeHeader;
	rangeHeader.reserve(13 + ranges.size());
	rangeHeader.append("Range: bytes=");
	rangeHeader.append(ranges);
	struct curl_slist* currHeader = nullptr;
	currHeader = curl_slist_append(currHeader, rangeHeader.c_str());
	headers.push_back(currHeader);
      }
      for (auto j = 0; j < totalRequests; j += maxHandles) {
	auto numRequests = maxHandles < totalRequests - j ? maxHandles : totalRequests - j;

	for (size_t i = 0; i < numRequests; i++) {
	  CURL* easyHandle = curlMan.getEasyHandle();
	  if (easyHandle) {
	    auto& [ranges, estimatedSize] = rangeSets[j + i];
	    setUpEasyForMultiAndParseMulti(multiHandle, easyHandle, url,
					   headers[j + i], &writeBuffers[j + i], writeResponseFunc, curlMan.handleUsed(easyHandle));
	  }
	}

	int stillRunning = 0;
	curl_multi_perform(multiHandle, &stillRunning);
	
	while (stillRunning) {
	  int numfds = 0;
	  CURLMcode mc = curl_multi_poll(multiHandle, nullptr, 0, 500, &numfds);

	  if (mc != CURLM_OK) {
	    std::cerr << "curl_multi_wait() failed: " << curl_multi_strerror(mc) << std::endl;
	    break;
	  }

	  curl_multi_perform(multiHandle, &stillRunning);
	  int msgsLeft = 0;
	  CURLMsg* msg = nullptr;
	  while ((msg = curl_multi_info_read(multiHandle, &msgsLeft))) {
	    if (msg->msg == CURLMSG_DONE) {
	      CURL* easy = msg->easy_handle;
	      CURLcode res = msg->data.result;

	      WriteBuffer* wb = nullptr;
	      curl_easy_getinfo(easy, CURLINFO_PRIVATE, &wb);
	      
	      if (wb && wb->offset > 0) {
		wb->data.resize(wb->offset);
		ParsedTask task {
		  .data = std::move(wb->data),
		  .boundary = "*",
		  .bufferMan = &bufferMan
		};
		parseQueue.push(std::move(task));
		tasksEnqueued.fetch_add(1, std::memory_order_relaxed);
	      }
	      if (isTrackingOverhead) {
		trackEasyTransferData(bufferMan, easy, maxHandles);
	      }
	      
	      curl_multi_remove_handle(multiHandle, easy);
	      curlMan.releaseEasyHandle(easy);
	    }
	  }
	}
      }

      while (tasksParsed.load() < tasksEnqueued.load()) {
	size_t parsed = tasksParsed.load(std::memory_order_relaxed);
	size_t enqueued = tasksEnqueued.load(std::memory_order_relaxed);
#ifdef HEADERDEBUG
	std::cout << "[Monitor] Parsed: " << parsed
		  << " / Enqueued: " << enqueued << std::endl;
#endif
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }

      stopMonitoring = true;
      monitorThread.join();

      stopParsing = true;
      for (auto& thread : parserThreads) {
	thread.join();
      }

      for (size_t i = 0; i < totalRequests; i++) {
	if (headers[i]) {
	  curl_slist_free_all(headers[i]);
	}
      }

      ParsedTask dummy;
      while (parseQueue.tryPop(dummy)) {}
      tasksEnqueued = 0;
      tasksParsed = 0;
      stopParsing = false;
      parserThreads.clear();

    }

    void createMultithreadedRequests(BufferManager &bufferMan, std::vector<std::vector<std::pair<std::string, size_t>>> &&rangeSets, size_t (*writeResponseFunc)(void *, size_t, size_t, void *), bool isTrackingOverhead) {
      if (!multithreaded) {
	throw std::runtime_error("Erroneous attempt to call multithreaded function with single thread.");
      }
      currRangeSets = std::move(rangeSets);
      std::vector<std::future<void>> futures;
      if (threadRequestType == 1) {
	
	std::vector<std::future<void>> curlFutures;
	std::vector<std::thread> parserThreads;

	int32_t numParserThreads = std::max(2, int32_t(currNumThreads / 2));
	for (size_t i = 0; i < numParserThreads; i++) {
	  parserThreads.emplace_back(&RequestsManager::parserThreadLoop, this);
	}

        for (size_t i = 0; i < currNumThreads; i++) {
	  curlFutures.push_back(std::async(std::launch::async,
					   [this, &bufferMan, writeResponseFunc, isTrackingOverhead, i]() mutable {
					     for (auto const& [rangeStr, estimatedSize] : this->currRangeSets[i]) {
					       std::vector<char> responseBuffer(estimatedSize);
					       WriteBuffer wb{ std::move(responseBuffer), 0 };

					       allocateRangesFromURL(bufferMan, this->threadCurlHandles[i],
								     rangeStr, this->url,
								     nullptr, &wb, writeResponseFunc,
								     isTrackingOverhead);

					       wb.data.resize(wb.offset);
					       ParsedTask task{
						 .data = std::move(wb.data),
						 .boundary = "*",
						 .bufferMan = &bufferMan
					       };
					       parseQueue.push(std::move(task));
					       tasksEnqueued.fetch_add(1, std::memory_order_relaxed);
					     }
					   }));
	}

	for (auto& future : curlFutures) {
	  future.get();
	}

	while (tasksParsed.load() < tasksEnqueued.load()) {
	  std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}

	stopParsing = true;
	for (auto& thread : parserThreads) {
	  thread.join();
	}

	ParsedTask dummy;
	while (parseQueue.tryPop(dummy)) {}
	tasksEnqueued = 0;
	tasksParsed = 0;
	stopParsing = false;
	parserThreads.clear();

      } else if (threadRequestType == 2) {
	for (size_t i = 0; i < currNumThreads; i++) {
	  futures.push_back(std::async(std::launch::async,
				       [this, &bufferMan, writeResponseFunc, isTrackingOverhead, i]() mutable {
					 allocateRangesWithMulti(bufferMan, this->threadCurlManagers[i], this->currRangeSets[i], this->url, writeResponseFunc, isTrackingOverhead);
				       }));
	}
      } else if (threadRequestType == 3) {
	size_t managersThreads = (size_t) std::floor((double_t) currNumThreads / 4.0);
	size_t handlesThreads = currNumThreads - managersThreads;
	size_t currRangeSetI = 0;
	for (size_t i = 0; i < managersThreads && currRangeSetI < currNumThreads; i++, currRangeSetI++) {
	  futures.push_back(std::async(std::launch::async,
				       [this, &bufferMan, writeResponseFunc, isTrackingOverhead, i, currRangeSetI]() mutable {
					 allocateRangesWithMulti(bufferMan, this->threadCurlManagers[i], this->currRangeSets[currRangeSetI], this->url, writeResponseFunc, isTrackingOverhead);
				       }));
	}
	for (size_t i = 0; i < handlesThreads && currRangeSetI < currNumThreads; i++, currRangeSetI++) {
	  futures.push_back(std::async(std::launch::async,
				       [this, &bufferMan, writeResponseFunc, isTrackingOverhead, i, currRangeSetI]() mutable {
					 allocateRangesWithEasy(bufferMan, this->threadCurlHandles[i], this->currRangeSets[currRangeSetI], this->url, writeResponseFunc, isTrackingOverhead);
				       }));
	}
      }

      for (auto &future : futures) {
	future.get();
      }
    }

    RequestsManager(const RequestsManager&) = delete;
    RequestsManager& operator=(const RequestsManager&) = delete;
    
    RequestsManager(RequestsManager&& other) noexcept
      : currNumThreads(other.currNumThreads),
	currNumHandles(other.currNumHandles),
	multithreaded(other.multithreaded),
	threadRequestType(other.threadRequestType),
	url(std::move(other.url)),
	currRangeSets(std::move(other.currRangeSets)),
	existsInfo(other.existsInfo),
	mainHandle(other.mainHandle),
	mainManager(std::move(other.mainManager)),
	threadCurlManagers(std::move(other.threadCurlManagers)),
	threadCurlHandles(std::move(other.threadCurlHandles)) {
      other.mainHandle = nullptr;
    }

    RequestsManager& operator=(RequestsManager&& other) noexcept {
      if (this != &other) {
	currNumThreads = other.currNumThreads;
	currNumHandles = other.currNumHandles;
	multithreaded = other.multithreaded;
	threadRequestType = other.threadRequestType;
        url = std::move(other.url);
	currRangeSets = std::move(other.currRangeSets);
	existsInfo = other.existsInfo;
	mainHandle = other.mainHandle;
	mainManager = std::move(other.mainManager);
	threadCurlManagers = std::move(other.threadCurlManagers);
	threadCurlHandles = std::move(other.threadCurlHandles);
	other.currNumThreads = 0;
	other.currNumHandles = 0;
	other.multithreaded = false;
	other.mainHandle = nullptr;
      }
      return *this;
    }

    void cleanup() {
      curl_easy_cleanup(mainHandle);
      mainManager.cleanup();
      for (auto &curlHandle : threadCurlHandles) {
	curl_easy_cleanup(curlHandle);
      }
      for (auto &curlManager : threadCurlManagers) {
	curlManager.cleanup();
      }
    }

    ~RequestsManager() {
      cleanup();
    }
    
  };


private:
  bool curlGlobalSet = false;
  bool isTrackingOverhead = false;
  bool isTrackingRequired = false;

  size_t NUM_THREADS = 64;
  size_t NUM_HANDLES = 20;
  
  std::string currURL;
  
  std::unordered_map<std::string, CURL *> curlHandlesMap;
  std::unordered_map<std::string, RequestsManager> requestsManagersMap;
  std::unordered_map<std::string, BufferManager> bufferMap;
  std::unordered_map<std::string, DummyBufferManager> overheadBufferMap;

  static URLExistenceInfo checkURLExists(CURL *curl, std::string const &url);

  std::vector<std::pair<int64_t, int64_t>> extractBoundPairs(
      const std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
      DummyBufferManager &buffMan);
  std::vector<std::pair<int64_t, int64_t>>
  extractBoundPairs(std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
                    BufferManager &buffMan, int64_t padding, int64_t alignment, int64_t ranges, int64_t requests);

  static void trackEasyTransferData(BufferManager &bufferMan, CURL* curl, size_t numHandles);

  static void setUpEasyForMulti(MultipartResponseHandler &responseHandler, CURLM *multiCurl, CURL *curl,
				 std::string const &ranges,
				 std::string const &url,
				 size_t (*writeResponseFunc)(void *, size_t,
							     size_t, void *));

  static void allocateRangesWithMulti(BufferManager &bufferMan, CurlManager &curlMan,
				      std::vector<std::pair<std::string, size_t>> &rangeSets, 
				       std::string const &url,
				       size_t (*writeResponseFunc)(void *, size_t,
								   size_t, void *),
				       bool isTrackingOverhead);
  
  static void allocateRangesWithEasy(BufferManager &bufferMan, CURL* curlHand,
				     std::vector<std::pair<std::string, size_t>> const &rangeSets, 
				       std::string const &url,
				       size_t (*writeResponseFunc)(void *, size_t,
								   size_t, void *),
				       bool isTrackingOverhead);
  
  static void allocateRangeFromURL(BufferManager &bufferMan, CURL *curl,
                                   std::string const &range,
                                   std::string const &url,
                                   size_t (*writeResponseFunc)(void *, size_t,
                                                               size_t, void *),
                                   bool isTrackingOverhead);

  static void allocateRangeFromURL(BufferManager &bufferMan, WriteBufferDirect &wbd, CURL *curl,
                                   std::string const &range,
                                   std::string const &url,
                                   size_t (*writeResponseFunc)(void *, size_t,
                                                               size_t, void *),
                                   bool isTrackingOverhead);

  static void allocateRangesFromURL(BufferManager &bufferMan, CURL *curl,
                                    std::string const &range,
                                    std::string const &url,
                                    size_t (*writeResponseFunc)(void *, size_t,
                                                                size_t, void *),
                                    bool isTrackingOverhead);

  static void allocateRangesFromURL(BufferManager &bufferMan, CURL *curl,
                                    std::string const &range,
                                    std::string const &url,
				    void *headerData, void *writeData,
				    size_t (*writeResponseFunc)(void *, size_t,
                                                                size_t, void *),
                                    bool isTrackingOverhead,
				    bool reset = true);

  static uint64_t getFileLength(std::string const &url, CURL *curl);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::RBL
