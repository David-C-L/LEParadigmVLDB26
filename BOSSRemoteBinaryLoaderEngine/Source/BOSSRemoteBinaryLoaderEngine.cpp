#include "BOSSRemoteBinaryLoaderEngine.hpp"
#include "MultipartRangesResponseParser.hpp"
#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <mutex>
#include <ostream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <cmath>
#include <future>
#include <thread>

#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

#ifdef _WIN32
// curl includes windows headers without using NOMINMAX (causing issues with
// std::min/max)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif //_WIN32
#include <curl/curl.h>

// #define DEBUG

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::RBL {
constexpr static size_t const MULTIPART_OVERHEAD_ESTIMATE = 140;
constexpr static size_t const HEADERS_OVERHEAD_ESTIMATE = 500;
constexpr static int64_t const DEFAULT_PADDING = 0;
constexpr static int64_t const DEFAULT_ALIGNMENT = 1;
constexpr static int64_t const DEFAULT_MAX_RANGES = 1;
constexpr static int64_t const UNLIMITED_MAX_RANGES = -1;
constexpr static int64_t const DEFAULT_MAX_REQUESTS = 1;
constexpr static int64_t const UNLIMITED_MAX_REQUESTS = -1;
constexpr static int64_t const DEFAULT_THREADS = 8;
constexpr static bool const DEFAULT_TRACKING_CACHE = true;
static std::string const BOUNDARY_KEY = "boundary=";
  std::mutex printMutex;
using std::move;
using WriteBuffer = Engine::WriteBuffer;
using WriteBufferDirect = Engine::WriteBufferDirect;
using BufferManager = Engine::BufferManager;
using MultipartResponseHandler = Engine::MultipartResponseHandler;
using URLExistenceInfo = Engine::URLExistenceInfo;
namespace utilities {} // namespace utilities
  
static size_t writeDataDefault(void *buffer, size_t size, size_t nmemb,
                               void *userp) {
  return size * nmemb;
};

static size_t writeDataToSimpleBuffer(void *buffer, size_t size, size_t nmemb,
					void *userp) {
  const size_t total = size * nmemb;

  WriteBuffer* buf = static_cast<WriteBuffer*>(userp);
  const size_t bufCapacity = buf->data.size();
  
  if (buf->offset + total > bufCapacity) {
    return 0;
  }

  std::memcpy(buf->data.data() + buf->offset, buffer, total);
  buf->offset += total;
  return total;
};

static size_t writeRangeToBuffer(void *contents, size_t size, size_t nmemb,
                                 void *userp) {
  BufferManager *bufferMan = static_cast<BufferManager *>(userp);
  size_t size_b = size * nmemb;
  memcpy((bufferMan->buffer + bufferMan->currentOffset), contents, size_b);
  bufferMan->currentOffset += size_b;
  return size_b;
};
  
static size_t writeRangeToBufferDirect(void *contents, size_t size, size_t nmemb,
                                 void *userp) {
  WriteBufferDirect *bufferMan = static_cast<WriteBufferDirect *>(userp);
  size_t size_b = size * nmemb;
  memcpy((bufferMan->buffer + bufferMan->offset), contents, size_b);
  bufferMan->offset += size_b;
  return size_b;
};

static size_t writeBoundaryString(void *contents, size_t size, size_t nmemb,
                                  void *userp) {
  if (!userp) {
    return size * nmemb;
  }
  
  char *data = static_cast<char *>(contents);
  size_t size_b = size * nmemb;
  std::string header(data, size_b);

  size_t start = header.find(BOUNDARY_KEY);
  if (start != std::string::npos) {
    start += BOUNDARY_KEY.length();
    std::string boundary = header.substr(start);
    boundary.erase(remove(boundary.begin(), boundary.end(), '\r'),
                   boundary.end());
    boundary.erase(remove(boundary.begin(), boundary.end(), '\n'),
                   boundary.end());
#ifdef DEBUG
    std::cout << "CURRENT BOUNDARY: " << boundary << std::endl;
#endif
    boundary = "--" + boundary;
    MultipartResponseHandler *responseHandler =
        static_cast<MultipartResponseHandler *>(userp);
    responseHandler->resetBoundary(std::move(boundary));
  }
  return size_b;
};

static size_t writeMultipartToBuffer(void *contents, size_t size, size_t nmemb,
                                     void *userp) {
  MultipartResponseHandler *responseHandler =
      static_cast<MultipartResponseHandler *>(userp);
  char *data = static_cast<char *>(contents);
  size_t size_b = size * nmemb;

  auto res = responseHandler->handleMultipart(data, size_b, 0);

  return res;
}

  URLExistenceInfo Engine::checkURLExists(CURL *curl, std::string const &url) {
    CURLcode res;
    int64_t responseCode = 0;
    int64_t requestSize = 0;
    int64_t headerSize = 0;
    int64_t pretransferTime = 0;
    int64_t totalTime = 0;
    
    if (curl) {
      curl_easy_reset(curl);
      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
      curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

      
#ifdef DEBUG
    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif
    
      res = curl_easy_perform(curl);

      if (res == CURLE_OK) {
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
        curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T, &pretransferTime);
        curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T,
                                &totalTime);
      }
    }

    int64_t downloadTime = totalTime - pretransferTime;

    return { responseCode == 200, requestSize, headerSize, pretransferTime, downloadTime };
  }

void Engine::allocateRangeFromURL(BufferManager &bufferMan, CURL *curl,
                                  std::string const &range,
                                  std::string const &url,
                                  size_t (*writeResponseFunc)(void *, size_t,
                                                              size_t, void *),
                                  bool isTrackingOverhead) {
  CURLcode res;

  if (curl) {
    curl_easy_reset(curl);

#ifdef DEBUG
    std::cout << "allocateRangeFromURL -- RANGE LEN:" << range.size() << std::endl;
    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif

    curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
    curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);

    // RANGE CONSTRUCTION
    const char* prefix = "Range: bytes=";
    const size_t prefixLen = strlen(prefix);
    size_t headerLen = prefixLen + range.size();
    char* rangeHeader = (char*)malloc(headerLen + 1);
    if (!rangeHeader) {
      throw std::runtime_error("Failed to malloc header!");
    }
    std::memcpy(rangeHeader, prefix, prefixLen);
    std::memcpy(rangeHeader + prefixLen, range.data(), range.size());
    rangeHeader[headerLen] = '\0';

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, rangeHeader);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &bufferMan);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    
    curl_easy_setopt(curl, CURLOPT_UPLOAD_BUFFERSIZE, 64*1024*1024);
    curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 64*1024*1024);
    
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    res = curl_easy_perform(curl);

    if (headers) {
      curl_slist_free_all(headers);
    }
    
    if (isTrackingOverhead) {
      if (res == CURLE_OK) {
        curl_off_t downloadSize = 0;
        int64_t requestSize = 0;
        int64_t headerSize = 0;
        curl_off_t pretransferTime = 0;
        curl_off_t totalTime = 0;

#ifdef DEBUG
        std::cout << "COLLECTING DATA" << std::endl;
#endif

        res = curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD_T, &downloadSize);
        res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
                                &pretransferTime);
        res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);	

#ifdef DEBUG
        std::cout << "COLLECTED DATA" << std::endl;
#endif

        bufferMan.totalDownloaded += downloadSize;
        bufferMan.totalRequested += requestSize;
        bufferMan.totalHeadersDownloaded += headerSize;
        bufferMan.totalPretransferTime += pretransferTime;
        bufferMan.totalDownloadTime += totalTime - pretransferTime;

#ifdef DEBUG
        std::cout << "ADDED DATA TO BUFFER MANAGER" << std::endl;
#endif
      }
    }

#ifdef DEBUG
    if (res != CURLE_OK) {
      std::cout << "ERROR CODE:" << std::endl;
      std::cout << res << std::endl;
      std::cout << "ERROR BUFFER:" << std::endl;
      std::cout << errbuf << std::endl;
    }
#endif
  }
};
  
  void Engine::allocateRangeFromURL(BufferManager &bufferMan, WriteBufferDirect &wbd, CURL *curl,
                                  std::string const &range,
                                  std::string const &url,
                                  size_t (*writeResponseFunc)(void *, size_t,
                                                              size_t, void *),
                                  bool isTrackingOverhead) {
  CURLcode res;

  if (curl) {
    curl_easy_reset(curl);

#ifdef DEBUG
    std::cout << "allocateRangeFromURL -- RANGE LEN:" << range.size() << std::endl;
    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif

    curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
    curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);

    // RANGE CONSTRUCTION
    const char* prefix = "Range: bytes=";
    const size_t prefixLen = strlen(prefix);
    size_t headerLen = prefixLen + range.size();
    char* rangeHeader = (char*)malloc(headerLen + 1);
    if (!rangeHeader) {
      throw std::runtime_error("Failed to malloc header!");
    }
    std::memcpy(rangeHeader, prefix, prefixLen);
    std::memcpy(rangeHeader + prefixLen, range.data(), range.size());
    rangeHeader[headerLen] = '\0';

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, rangeHeader);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &wbd);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    
    curl_easy_setopt(curl, CURLOPT_UPLOAD_BUFFERSIZE, 64*1024*1024);
    curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 64*1024*1024);
    
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    res = curl_easy_perform(curl);

    if (headers) {
      curl_slist_free_all(headers);
    }
    
    if (isTrackingOverhead) {
      if (res == CURLE_OK) {
        curl_off_t downloadSize = 0;
        int64_t requestSize = 0;
        int64_t headerSize = 0;
        curl_off_t pretransferTime = 0;
        curl_off_t totalTime = 0;

#ifdef DEBUG
        std::cout << "COLLECTING DATA" << std::endl;
#endif

        res = curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD_T, &downloadSize);
        res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
                                &pretransferTime);
        res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);

#ifdef DEBUG
        std::cout << "COLLECTED DATA" << std::endl;
#endif

        bufferMan.totalDownloaded += downloadSize;
        bufferMan.totalRequested += requestSize;
        bufferMan.totalHeadersDownloaded += headerSize;
        bufferMan.totalPretransferTime += pretransferTime;
        bufferMan.totalDownloadTime += totalTime - pretransferTime;

#ifdef DEBUG
        std::cout << "ADDED DATA TO BUFFER MANAGER" << std::endl;
#endif
      }
    }

#ifdef DEBUG
    if (res != CURLE_OK) {
      std::cout << "ERROR CODE:" << std::endl;
      std::cout << res << std::endl;
      std::cout << "ERROR BUFFER:" << std::endl;
      std::cout << errbuf << std::endl;
    }
#endif
  }
};

void Engine::allocateRangesFromURL(BufferManager &bufferMan, CURL *curl,
				   std::string const &ranges,
				   std::string const &url,
				   void* headerData, void* writeData,
				   size_t (*writeResponseFunc)(void *, size_t,
							       size_t, void *),
				   bool isTrackingOverhead,
				   bool reset) {
  
  CURLcode res;

  if (curl) {
    
    if (reset) {      
      curl_easy_reset(curl);

      curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
      curl_easy_setopt(curl, CURLOPT_TCP_FASTOPEN, 1L);
      
      curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
      curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
      curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);
      
      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());	  

      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    
      curl_easy_setopt(curl, CURLOPT_UPLOAD_BUFFERSIZE, 64*1024*1024);
      curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 64*1024*1024);
    
      curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
      curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    }
    
#ifdef DEBUG
    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif

    // RANGE CONSTRUCTION
    const char* prefix = "Range: bytes=";
    const size_t prefixLen = strlen(prefix);
    size_t headerLen = prefixLen + ranges.size();
    char* rangeHeader = (char*)malloc(headerLen + 1);
    if (!rangeHeader) {
      throw std::runtime_error("Failed to malloc header!");
    }
    std::memcpy(rangeHeader, prefix, prefixLen);
    std::memcpy(rangeHeader + prefixLen, ranges.data(), ranges.size());
    rangeHeader[headerLen] = '\0';
    
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, rangeHeader);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    if (headerData) {
      curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, &writeBoundaryString);
      curl_easy_setopt(curl, CURLOPT_HEADERDATA, headerData);
    }
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, writeData);

    res = curl_easy_perform(curl);

    if (headers) {
      curl_slist_free_all(headers);
    }

    if (isTrackingOverhead || true) {
      if (res == CURLE_OK) {

	curl_off_t downloadSize = 0;
        int64_t requestSize = 0;
        int64_t headerSize = 0;
        curl_off_t pretransferTime = 0;
        curl_off_t totalTime = 0;

#ifdef DEBUG
        std::cout << "COLLECTING DATA" << std::endl;
#endif

        res = curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD_T, &downloadSize);
        res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
                                &pretransferTime);
        res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);
#ifdef DEBUG
        std::cout << "COLLECTED DATA" << std::endl;
#endif

        bufferMan.totalDownloaded += downloadSize;
        bufferMan.totalRequested += requestSize;
        bufferMan.totalHeadersDownloaded += headerSize;
        bufferMan.totalPretransferTime += pretransferTime;
        bufferMan.totalDownloadTime += totalTime - pretransferTime;

#ifdef DEBUG
        std::cout << "ADDED DATA TO BUFFER MANAGER" << std::endl;
#endif
      }
    }

#ifdef DEBUG
    if (res != CURLE_OK) {
      std::cout << "ERROR CODE:" << std::endl;
      std::cout << res << std::endl;
      std::cout << "ERROR BUFFER:" << std::endl;
      std::cout << errbuf << std::endl;
    }
#endif
  }
};

void Engine::allocateRangesFromURL(BufferManager &bufferMan, CURL *curl,
                                   std::string const &ranges,
                                   std::string const &url,
                                   size_t (*writeResponseFunc)(void *, size_t,
                                                               size_t, void *),
                                   bool isTrackingOverhead) {
  MultipartResponseHandler responseHandler(&bufferMan);
  allocateRangesFromURL(bufferMan, curl, ranges, url, &responseHandler, &responseHandler, writeResponseFunc, isTrackingOverhead);
};

  void Engine::allocateRangesWithEasy(BufferManager &bufferMan, CURL* curlHand,
				      std::vector<std::pair<std::string, size_t>> const &rangeSets, 
				       std::string const &url,
				       size_t (*writeResponseFunc)(void *, size_t,
								   size_t, void *),
				      bool isTrackingOverhead) {
    for (auto const &[rangeSet, estimateSize] : rangeSets) {
      allocateRangesFromURL(bufferMan, curlHand, rangeSet, url, writeResponseFunc, isTrackingOverhead);
    }
  };

  void Engine::trackEasyTransferData(BufferManager &bufferMan, CURL* curl, size_t numHandles = 1) {
    if (curl) {
      CURLcode res;
      curl_off_t downloadSize = 0;
      int64_t requestSize = 0;
      int64_t headerSize = 0;
      curl_off_t pretransferTime = 0;
      curl_off_t totalTime = 0;

#ifdef DEBUG
      std::cout << "COLLECTING DATA" << std::endl;
#endif

      res = curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD_T, &downloadSize);
      res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
      res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
      res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
			      &pretransferTime);
      res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);

#ifdef DEBUG
      std::cout << "COLLECTED DATA" << std::endl;
#endif

      bufferMan.totalDownloaded += downloadSize;
      bufferMan.totalRequested += requestSize;
      bufferMan.totalHeadersDownloaded += headerSize;
      bufferMan.totalPretransferTime += pretransferTime / static_cast<double>(numHandles);
      bufferMan.totalDownloadTime += (totalTime / static_cast<double>(numHandles)) - (pretransferTime / static_cast<double>(numHandles));

#ifdef DEBUG
      std::cout << "ADDED DATA TO BUFFER MANAGER" << std::endl;
#endif
    }
  }

  void Engine::setUpEasyForMulti(MultipartResponseHandler& responseHandler,
				 CURLM *multiCurl, CURL *curl,
				 std::string const &ranges,
				 std::string const &url,
				 size_t (*writeResponseFunc)(void *, size_t,
							     size_t, void *)) {
    if (curl) {
      curl_easy_reset(curl);

      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
      curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);
      curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 600L);

      // RANGE CONSTRUCTION
      std::string rangeHeader;
      rangeHeader.reserve(13 + ranges.size());
      rangeHeader.append("Range: bytes=");
      rangeHeader.append(ranges);
      struct curl_slist* currHeader = nullptr;
      currHeader = curl_slist_append(currHeader, rangeHeader.c_str());
      responseHandler.attachedHeaderList = currHeader;

      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, currHeader);
      
      curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, &writeBoundaryString);
      curl_easy_setopt(curl, CURLOPT_HEADERDATA, &responseHandler);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseHandler);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

      curl_easy_setopt(curl, CURLOPT_UPLOAD_BUFFERSIZE, 64*1024*1024);
      curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 64*1024*1024);
    
      curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
      curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);


      curl_multi_add_handle(multiCurl, curl);
    }
  }
  
  void Engine::allocateRangesWithMulti(BufferManager &bufferMan, CurlManager &curlMan,
				       std::vector<std::pair<std::string, size_t>> &rangeSets, 
				       std::string const &url,
				       size_t (*writeResponseFunc)(void *, size_t,
								   size_t, void *),
				       bool isTrackingOverhead) {

    auto totalRequests = rangeSets.size();
    auto maxHandles = curlMan.maxHandles;
#ifdef DEBUG
    std::cout << "TOTAL REQUESTS: " << totalRequests << " MAX HANDLES: " << maxHandles << std::endl;
#endif
    CURLM* multiHandle = curlMan.getMultiHandle();

    for (auto j = 0; j < totalRequests; j += maxHandles) {
      auto numRequests = maxHandles < totalRequests - j ? maxHandles : totalRequests - j;
      std::vector<MultipartResponseHandler> responseHandlers;
      std::vector<CURL*> usedEasyHandles;
      
      for (size_t i = 0; i < numRequests; i++) {
	MultipartResponseHandler responseHandler(&bufferMan);
	responseHandlers.push_back(std::move(responseHandler));
      }
    
      for (size_t i = 0; i < numRequests; i++) {
	CURL* easyHandle = curlMan.getEasyHandle();
	if (easyHandle) {
	  auto& [ranges, estimatedSize] = rangeSets[j + i];
	  setUpEasyForMulti(responseHandlers[i], multiHandle, easyHandle, ranges, url,
			    writeResponseFunc);
	  usedEasyHandles.push_back(easyHandle);
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
      }

      for (size_t i = 0; i < numRequests; i++) {
	CURL* easyHandle = usedEasyHandles[i];
	if (easyHandle) {
	  if (isTrackingOverhead) {
	    trackEasyTransferData(bufferMan, easyHandle);
	  }

	  MultipartResponseHandler& responseHandler = responseHandlers[i];
	  if (responseHandler.attachedHeaderList) {
	    curl_slist_free_all(responseHandler.attachedHeaderList);
	    responseHandler.attachedHeaderList = nullptr;
	  }
	  
	  curl_multi_remove_handle(multiHandle, easyHandle);
	  curlMan.releaseEasyHandle(easyHandle);
	}
      }
    }
  }  

uint64_t Engine::getFileLength(std::string const &url, CURL *curl) {
  CURLcode res;
  curl_off_t fileSize = 0;

  if (curl) {
    curl_easy_reset(curl);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeDataDefault);
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res)
                << std::endl;
    } else {
      res =
          curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &fileSize);
      if ((res != CURLE_OK) || (fileSize <= 0)) {
        std::cerr << "Error: Failed to retrieve file size or file size unknown."
                  << std::endl;
      }
    }
  }
  return static_cast<uint64_t>(fileSize);
};

std::vector<std::pair<int64_t, int64_t>>
mergeBounds(std::vector<std::pair<int64_t, int64_t>> &intervals,
            int64_t padding, int64_t alignment) {
  std::for_each(intervals.begin(), intervals.end(),
		[&padding](auto &a) {
		  a.second += padding;
		});
  std::for_each(intervals.begin(), intervals.end(),
                [&alignment](auto &a) {
		  a.first -= a.first % alignment;
		  a.second += alignment - (a.second % alignment);
		});
  std::sort(
      intervals.begin(), intervals.end(),
      [](const std::pair<int64_t, int64_t> &a,
         const std::pair<int64_t, int64_t> &b) { return a.first < b.first; });

  std::vector<std::pair<int64_t, int64_t>> merged;
  for (auto &[first, second] : intervals) {
    if (merged.empty() || merged.back().second < first) {
      merged.emplace_back(first, second);
    } else {
      merged.back().second = std::max(merged.back().second, second);
    }
  }
  return merged;
}

  std::vector<std::pair<int64_t, int64_t>> alignBoundsToRanges(std::vector<std::pair<int64_t, int64_t>> &bounds, int64_t ranges, int64_t requests) {
    auto totalRanges = ranges * requests;
    if (totalRanges < 1 || ranges < 1 || totalRanges > bounds.size()) {
      return bounds;
    }
    auto boundsPerRange = std::ceil(bounds.size() / totalRanges);

    std::vector<std::pair<int64_t, int64_t>> rangeMerged;
    rangeMerged.reserve(totalRanges);

    for (auto currBoundI = 0; currBoundI < bounds.size(); currBoundI += boundsPerRange) {
      auto possibleNextI = currBoundI + boundsPerRange;
      auto lastI = possibleNextI < bounds.size() ? possibleNextI - 1 : bounds.size() - 1;
      auto const &[firstLB, firstUB] = bounds[currBoundI];
      auto const &[lastLB, lastUB] = bounds[lastI];
      rangeMerged.emplace_back(firstLB, lastUB);
    }
    
    return std::move(rangeMerged);
  }

std::vector<std::pair<int64_t, int64_t>> Engine::extractBoundPairs(
    std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
    BufferManager &buffMan, int64_t padding, int64_t alignment, int64_t ranges, int64_t requests) {

  
#ifdef DEBUG
  std::cout << "REQUESTED COUNT: " << requestedBounds.size() << std::endl;
  std::cout << "RANGES: " << ranges << " REQUESTS: " << requests << std::endl;

#endif
  std::vector<std::pair<int64_t, int64_t>> mergedBounds =
    mergeBounds(requestedBounds, padding, alignment);

#ifdef DEBUG
  std::cout << "MERGED: " << mergedBounds.size() << std::endl;
#endif
  std::vector<std::pair<int64_t, int64_t>> rangeMergedBounds =
    alignBoundsToRanges(mergedBounds, ranges, requests);
#ifdef DEBUG
  std::cout << "RANGE MERGED: " << rangeMergedBounds.size() << std::endl;
#endif
  std::vector<std::pair<int64_t, int64_t>> neededBounds;
  for (const auto &[lb, ub] : rangeMergedBounds) {
    auto validBounds = buffMan.requestBounds(lb, ub);
    neededBounds.insert(neededBounds.end(),
                        std::make_move_iterator(validBounds.begin()),
                        std::make_move_iterator(validBounds.end()));
  }
#ifdef DEBUG
  std::cout << "NEEDED: " << neededBounds.size() << std::endl;
#endif
  return neededBounds;
}

std::vector<std::pair<int64_t, int64_t>> Engine::extractBoundPairs(
    const std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
    DummyBufferManager &buffMan) {

  std::vector<std::pair<int64_t, int64_t>> neededBounds;
  for (const auto &[lb, ub] : requestedBounds) {
    auto validBounds = buffMan.requestBounds(lb, ub);
    neededBounds.insert(neededBounds.end(),
                        std::make_move_iterator(validBounds.begin()),
                        std::make_move_iterator(validBounds.end()));
  }
#ifdef DEBUG
  std::cout << "NEEDED: " << neededBounds.size() << std::endl;
#endif
  return neededBounds;
}

std::vector<std::pair<int64_t, int64_t>>
extractBoundPairsFromExpression(ComplexExpression &&e) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();
  if (head != "List"_) {
    throw std::runtime_error("Cannot extract bounds from non-List expression");
  }

  std::vector<std::pair<int64_t, int64_t>> requestedBounds;
  if (spans.size() != 0) {
    size_t numBounds = std::accumulate(
      spans.begin(), spans.end(), (size_t)0,
      [](auto runningSum, auto const &argument) {
        if (std::holds_alternative<boss::Span<int64_t>>(argument)) {
          return runningSum + get<boss::Span<int64_t>>(argument).size();
        }
        return runningSum;
      });
    if (numBounds % 2 == 0) {
      requestedBounds.reserve((numBounds / 2));
    }
    
    for (auto it = std::make_move_iterator(spans.begin());
         it != std::make_move_iterator(spans.end()); std::advance(it, 1)) {
      if (std::holds_alternative<boss::Span<int64_t>>(*it)) {
        auto typedSpan = get<boss::Span<int64_t>>(*it);

        if (typedSpan.size() % 2 != 0) {
          throw std::runtime_error("Bounds spans must contain an even number "
                                   "of elements to represent "
                                   "lower and upper bound pairs");
        }
        for (auto spanIt = std::make_move_iterator(typedSpan.begin());
             spanIt != std::make_move_iterator(typedSpan.end());
             std::advance(spanIt, 2)) {
          auto const &lb = *spanIt;
          auto const &ub = *(spanIt + 1);
          requestedBounds.emplace_back(lb, ub);
        }
      }
    }
  } else {
    if (dynamics.size() % 2 != 0) {
      throw std::runtime_error(
          "Bounds lists must contain an even number of elements to represent "
          "lower and upper bound pairs");
    }
    requestedBounds.reserve((dynamics.size() / 2));

    for (auto it = dynamics.begin(); it != dynamics.end();
         std::advance(it, 2)) {
      auto const &lb = get<int64_t>(*it);
      auto const &ub = get<int64_t>(*(it + 1));
      requestedBounds.emplace_back(lb, ub);
    }
  }

  return std::move(requestedBounds);
}

  void writePairsToFile(const std::vector<std::pair<int64_t, int64_t>> &pairs, const std::string &filename) {
    std::ofstream outFile(filename, std::ios::app);

    int64_t numPages = 0;
    for (const auto& [first, second] : pairs) {
      int64_t diff = second - first;
      numPages += (diff / 4096);
    }

    if (!outFile.is_open()) {
      std::cerr << "Could not open file " << filename << std::endl;
      return;
    }

    outFile << numPages << "\n";
  }

  static const char DIGIT_PAIRS[201] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

  inline __attribute__((always_inline)) char* uint64ToAscii(char* out, uint64_t value) {
    if (value == 0) {
      *out++ = '0';
      return out;
    }
    
    constexpr size_t MAX_DIGITS = 20;
    char buffer[MAX_DIGITS];
    char* p = buffer + MAX_DIGITS;

    while (value >= 100) {
      uint64_t const index = (value % 100) * 2;
      value /= 100;
      *--p = DIGIT_PAIRS[index + 1];
      *--p = DIGIT_PAIRS[index];
    }

    if (value < 10)
      *--p = '0' + static_cast<char>(value);
    else {
      uint64_t const index = value * 2;
      *--p = DIGIT_PAIRS[index + 1];
      *--p = DIGIT_PAIRS[index];
    }
    
    size_t len = buffer + MAX_DIGITS - p;
    std::memcpy(out, p, len);
    
    return out + len;
  }

  std::string formatRanges(const std::vector<std::pair<int64_t, int64_t>>& ranges) {
    constexpr size_t PER_PAIR_ESTIMATE = 48;
    const size_t rangesSize = ranges.size();
    const size_t buffSize = rangesSize * PER_PAIR_ESTIMATE;
    
    std::string result;
    result.resize(buffSize);

    char* ptr = result.data();

    for (size_t i = 0; i < rangesSize; i++) {
      const auto& [first, second] = ranges[i];
      ptr = uint64ToAscii(ptr, static_cast<uint64_t>(first));
      *ptr++ = '-';
      ptr = uint64ToAscii(ptr, static_cast<uint64_t>(second));
      if (i + 1 < rangesSize) {
	*ptr++ = ',';
      }
    }

    result.resize(ptr - result.data());
    return result;
  }
  
  std::vector<std::pair<std::string, size_t>> formatRangesChunked(const std::vector<std::pair<int64_t, int64_t>>& ranges,
					       size_t maxRangesPerString, int64_t startI = -1, int64_t endI = -1) {
    if (maxRangesPerString == 0) {
      throw std::invalid_argument("maxRangesPerString must be > 0");
    }
    
    constexpr size_t MAX_BYTES_PER_RANGE = 48;
    const size_t total = ranges.size();

    size_t start = 0;
    if (startI >= 0 && startI < total && startI <= endI) {
      start = startI;
    }

    size_t end = total;
    if (endI >= 0 && endI < total && startI <= endI) {
      end = endI;
    }

    const size_t numRanges = end - start;

    std::vector<std::pair<std::string, size_t>> result;
    result.reserve(numRanges / maxRangesPerString);
    
    for (size_t i = start; i < end; i += maxRangesPerString) {
      size_t chunkSize = std::min(maxRangesPerString, end - i);
      std::string chunk;
      chunk.resize(chunkSize * MAX_BYTES_PER_RANGE);

      char* ptr = chunk.data();
      size_t chunkEnd = i + chunkSize;

      size_t totalRangeLen = 0;
      for (size_t j = i; j < chunkEnd; j++) {
	const auto &[first, second] = ranges[j];
	totalRangeLen += second - first + 1;
	ptr = uint64ToAscii(ptr, static_cast<uint64_t>(first));
	*ptr++ = '-';
	ptr = uint64ToAscii(ptr, static_cast<uint64_t>(second));
	if (j + 1 < chunkEnd) {
	  *ptr++ = ',';
	}
      }
      
      size_t estimatedSize = totalRangeLen +
	MULTIPART_OVERHEAD_ESTIMATE * chunkSize +
	HEADERS_OVERHEAD_ESTIMATE;

      chunk.resize(ptr - chunk.data());
      result.emplace_back(std::move(chunk), estimatedSize);
    }
    return result;
  }

  int64_t countRanges(const std::vector<std::pair<int64_t, int64_t>>& ranges) {
    int64_t total = 0;
    for (const auto& [lb, ub] : ranges) {
      total += ub - lb + 1;
    }
    return total;
  }

  void analyseByteRanges(const std::vector<std::pair<int64_t, int64_t>>& ranges,
			 int64_t startIndex = -1,
			 int64_t endIndex = -1) {
    if (ranges.empty()) {
      return;
    }

    int64_t actualStart = (startIndex == -1) ? 0 : startIndex;
    int64_t actualEnd = (endIndex == -1) ? ranges.size() : endIndex;

    if (actualStart < 0 || actualEnd > static_cast<int64_t>(ranges.size()) || actualStart >= actualEnd) {
      std::cerr << "Invalid start or end index.\n";
      return;
    }

    std::vector<std::pair<int64_t, int64_t>> selected(ranges.begin() + actualStart, ranges.begin() + actualEnd);

    // Sort ranges by start
    std::sort(selected.begin(), selected.end());

    int64_t totalCoveredBytes = 0;
    int64_t totalSpan = 0;
    int64_t gapCount = 0;
    int64_t totalGapSize = 0;
    int64_t largestGap = 0;
    int64_t largestContiguousBlock = 0;

    int64_t minStart = selected.front().first;
    int64_t maxEnd = selected.front().second;

    for (const auto& [start, end] : selected) {
      if (start >= end) continue;
      int64_t size = end - start;
      totalCoveredBytes += size;
      if (size > largestContiguousBlock) {
	largestContiguousBlock = size;
      }
      if (start > maxEnd) {
	int64_t gap = start - maxEnd;
	gapCount++;
	totalGapSize += gap;
	if (gap > largestGap) largestGap = gap;
      }
      maxEnd = std::max(maxEnd, end);
    }

    totalSpan = maxEnd - minStart;
    double sparsity = totalSpan > 0 ? 100.0 * (totalSpan - totalCoveredBytes) / totalSpan : 0.0;
    double contiguityRatio = totalCoveredBytes > 0 ? 100.0 * largestContiguousBlock / totalCoveredBytes : 0.0;
    double fragmentationScore = totalCoveredBytes > 0 ? static_cast<double>(selected.size() - 1) / totalCoveredBytes : 0.0;
    double averageRangeSize = selected.empty() ? 0.0 : static_cast<double>(totalCoveredBytes) / selected.size();
    
    std::cout << "Range Analysis:\n";
    std::cout << "Total span: " << totalSpan << " bytes\n";
    std::cout << "Total covered: " << totalCoveredBytes << " bytes\n";
    std::cout << "Sparsity: " << sparsity << "%\n";
    std::cout << "Number of gaps: " << gapCount << "\n";
    if (gapCount > 0) {
      std::cout << "Average gap size: " << (totalGapSize / gapCount) << " bytes\n";
      std::cout << "Largest gap: " << largestGap << " bytes\n";
    }
    
    std::cout << "Contiguity ratio: " << contiguityRatio << "%\n";
    std::cout << "Fragmentation score: " << fragmentationScore << "\n";
    std::cout << "Average range size: " << averageRangeSize << " bytes\n";
  }

  std::vector<std::vector<std::pair<std::string, size_t>>> formatRangesChunkedAndPartitioned(const std::vector<std::pair<int64_t, int64_t>>& ranges,
							    size_t maxRangesPerString, size_t numPartitions) {
    if (maxRangesPerString <= 0) {
      throw std::invalid_argument("maxRangesPerString must be > 0");
    }
    if (numPartitions <= 0) {
      throw std::invalid_argument("numPartitions must be > 0");
    }

    const int64_t totalBytes = countRanges(ranges);
    
    std::vector<std::vector<std::pair<std::string, size_t>>> result;
    const size_t total = ranges.size();
    const size_t rangesPerPartition = (total / numPartitions) + 1;
    for (size_t i = 0, rangesCovered = 0;
	 i < numPartitions && rangesCovered < total;
	 i++, rangesCovered += rangesPerPartition) {
      int64_t partStartI = rangesCovered;
      int64_t partEndI = std::min(rangesCovered + rangesPerPartition, total);

      result.push_back(std::move(formatRangesChunked(ranges, maxRangesPerString, partStartI, partEndI)));
    }

    return result;
  }

  std::vector<std::pair<std::string, size_t>> flattenFormattedRanges(std::vector<std::vector<std::pair<std::string, size_t>>> &&formattedRanges) {
    size_t numFormattedRanges = 0;
    for (const auto& formattedRange : formattedRanges) {
      numFormattedRanges += formattedRange.size();
    }
    
    std::vector<std::pair<std::string, size_t>> res(numFormattedRanges);

    size_t index = 0;
    for (auto& inner : formattedRanges) {
      for (auto& p : inner) {
	res[index++] = std::move(p);
      }
    }

    return res;
  }

  std::pair<std::string, size_t> simpleFormatRangesAndGetResponseSizeEstimate(std::vector<std::pair<int64_t, int64_t>>& ranges, size_t startI, size_t endI) {

    const size_t numRanges = ranges.size();
    if (startI > endI || startI > numRanges || endI > numRanges) {
      throw std::runtime_error("Cannot format ranges for start > end or start/end > size");
    }
    
    size_t total = 0;
    bool isFirst = true;
    std::stringstream rangesStream;
    
    for (; startI < endI; startI++) {
      const auto& [lowerBound, upperBound] = ranges[startI];
      total += upperBound - lowerBound + 1;
      if (isFirst) {
	rangesStream << lowerBound << "-" << (upperBound - 1);
	isFirst = false;
      } else {
	rangesStream << "," << lowerBound << "-" << (upperBound - 1);
      }
    }
    std::string rangesStr = rangesStream.str();
    size_t estimatedSize = total +
      MULTIPART_OVERHEAD_ESTIMATE * numRanges +
      HEADERS_OVERHEAD_ESTIMATE;
    return { std::move(rangesStr), estimatedSize };
  }

boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
	    if (!curlGlobalSet) {
	      curl_global_init(CURL_GLOBAL_DEFAULT);
	      curlGlobalSet = true;
	    }
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "Fetch"_) {
          if (std::holds_alternative<ComplexExpression>(dynamics[0])) {
                auto const &url = get<std::string>(dynamics[1]);
                int64_t padding = DEFAULT_PADDING;
                if (dynamics.size() > 2) {
                  padding = get<int64_t>(dynamics[2]);
                  padding = padding < 0 ? DEFAULT_PADDING : padding;
                };
                int64_t alignment = DEFAULT_ALIGNMENT;
                if (dynamics.size() > 3) {
                  alignment = get<int64_t>(dynamics[3]);
                  alignment = alignment < 1 ? DEFAULT_ALIGNMENT : alignment;
                };
                int64_t maxRanges = DEFAULT_MAX_RANGES;
                bool enableMultipart = false;
                if (dynamics.size() > 4) {
                  maxRanges = get<int64_t>(dynamics[4]);
                  maxRanges = maxRanges < 1 ? UNLIMITED_MAX_RANGES : maxRanges;
                }
                int64_t maxRequests = DEFAULT_MAX_REQUESTS;
                if (dynamics.size() > 5) {
                  maxRequests = get<int64_t>(dynamics[5]);
		  maxRequests = maxRequests < 1 ? UNLIMITED_MAX_REQUESTS : maxRequests;
                }
		bool trackingCache = DEFAULT_TRACKING_CACHE;
		if (dynamics.size() > 6) {
		  maxRequests = UNLIMITED_MAX_REQUESTS;
		  trackingCache = get<bool>(dynamics[6]);
		}
		NUM_THREADS = DEFAULT_THREADS;
		if (dynamics.size() > 7) {
		  NUM_THREADS = get<int64_t>(dynamics[7]);
		}
		
		auto requestsManagersIt = requestsManagersMap.find(url);
		if (requestsManagersIt == requestsManagersMap.end()) {
		  RequestsManager requestsMan(NUM_THREADS, NUM_HANDLES, url, 1);
		  requestsManagersMap.emplace(url, std::move(requestsMan));
		}
		RequestsManager &requestsMan = requestsManagersMap.find(url)->second;

		auto &existsInfo = requestsMan.existsInfo;
		auto &curl = requestsMan.mainHandle;
		auto &curlMan = requestsMan.mainManager;
  
                auto bufferIt = bufferMap.find(url);
                if (bufferIt == bufferMap.end()) {
		  auto const totalSize = getFileLength(url, curl);
                  bufferMap.emplace(url, Engine::BufferManager(totalSize, existsInfo.exists));
                  if (isTrackingOverhead && isTrackingRequired) {
                    overheadBufferMap.emplace(
                        url, Engine::DummyBufferManager(totalSize));
                  }
                }

                auto requestedBounds = extractBoundPairsFromExpression(
                    std::move(get<ComplexExpression>(dynamics[0])));

                if (isTrackingOverhead && isTrackingRequired) {
                  auto &dummyBufferMan = overheadBufferMap[url];
                  auto dummyBounds =
                      extractBoundPairs(requestedBounds, dummyBufferMan);
                  dummyBufferMan.addLoaded(dummyBounds);
                }

                auto &bufferMan = bufferMap[url];
		
		bufferMan.totalRequested += existsInfo.requested;
		bufferMan.totalHeadersDownloaded += existsInfo.headers;
		bufferMan.totalPretransferTime += existsInfo.pretransferTime;
		bufferMan.totalDownloadTime += existsInfo.downloadTime;
		if (!bufferMan.exists) {
		  throw std::runtime_error("Remote resource does not exist.");
		}

                auto bounds = trackingCache ? extractBoundPairs(requestedBounds, bufferMan,
								padding, alignment, maxRanges, maxRequests) :
		  std::move(requestedBounds);
		
                if (isTrackingOverhead) {
                  bufferMan.addLoaded(bounds);
                }

		auto numRequests = 0;
                if (bounds.size() > 1 && maxRanges != DEFAULT_MAX_RANGES) {
		  std::stringstream ranges;
                  bool isFirst = true;
                  if (maxRanges == UNLIMITED_MAX_RANGES) {
		    auto [rangesStr, estimatedSize] = simpleFormatRangesAndGetResponseSizeEstimate(bounds, 0, bounds.size());
		    std::vector<char> responseBuffer(estimatedSize);
		    WriteBuffer wb = { std::move(responseBuffer), 0 };

		    CURL* easyHandle = curlMan.getEasyHandle();
                    allocateRangesFromURL(bufferMan, easyHandle, rangesStr, url,
                                          nullptr, &wb, &writeDataToSimpleBuffer,
                                          isTrackingOverhead, true);
		    curlMan.releaseEasyHandle(easyHandle);
		    
		    std::string_view boundary = "*";
		    MultipartRangeParser parser(wb.data.data(), wb.offset, bufferMan.buffer, bufferMan.totalSize, boundary);
		    parser.parseAndApply();
		    
		    numRequests++;
                  } else {
		    
		    if (trackingCache) {
		      CURL* easyHandle = curlMan.getEasyHandle();
		      for (size_t i = 0; i < bounds.size(); i += maxRanges) {
			auto remaining = maxRanges < (bounds.size() - i)
			  ? maxRanges
			  : (bounds.size() - i);
        		auto [rangesStr, estimatedSize] =
			  simpleFormatRangesAndGetResponseSizeEstimate(bounds, i, i + remaining);
        		int64_t boundsLeft = bounds.size() - (i + remaining);
#ifdef DEBUG
		        std::cout << "     REQUEST NO: " << numRequests << " CURR BOUND I: " << i << " BOUNDS LEFT: " << boundsLeft << std::endl;
#endif
			
			std::vector<char> responseBuffer(estimatedSize);
			WriteBuffer wb = { std::move(responseBuffer), 0 };
			
			allocateRangesFromURL(bufferMan, easyHandle, rangesStr, url,
					      nullptr, &wb, &writeDataToSimpleBuffer,
					      isTrackingOverhead, true);
			std::string_view boundary = "*";
			MultipartRangeParser parser(wb.data.data(), wb.offset, bufferMan.buffer, bufferMan.totalSize, boundary);
			parser.parseAndApply();
	      		numRequests++;
		      }
		      curlMan.releaseEasyHandle(easyHandle);
		    } else {
		      bool multithreaded = requestsMan.multithreaded;
		      if (multithreaded) {
		        size_t numThreads = NUM_THREADS;
			std::vector<std::vector<std::pair<std::string, size_t>>> rangesToDo = formatRangesChunkedAndPartitioned(bounds, maxRanges, numThreads);
			requestsMan.createMultithreadedRequests(bufferMan, std::move(rangesToDo), &writeDataToSimpleBuffer, true);			
		      } else {
			std::vector<std::pair<std::string, size_t>> rangesToDo = formatRangesChunked(bounds, maxRanges);
			CURL* easyHandle = curlMan.getEasyHandle();
			for (auto const &[rangeSet, estimatedSize] : rangesToDo) {
			  std::vector<char> responseBuffer(estimatedSize);
			  WriteBuffer wb = { std::move(responseBuffer), 0 };
			  
			  allocateRangesFromURL(bufferMan, easyHandle, rangeSet, url,
						nullptr, &wb, &writeDataToSimpleBuffer,
						isTrackingOverhead, true);
			  std::string_view boundary = "*";
			  MultipartRangeParser parser(wb.data.data(), wb.offset, bufferMan.buffer, bufferMan.totalSize, boundary);
			  parser.parseAndApply();
			}
			curlMan.releaseEasyHandle(easyHandle);
		      }
		    }
                  }
                } else {
		  
		  CURL* easyHandle = curlMan.getEasyHandle();
		  for (const auto &[lowerBound, upperBound] : bounds) {
		    WriteBufferDirect wb = { bufferMan.buffer, static_cast<size_t>(lowerBound) };

		    allocateRangeFromURL(bufferMan, wb, easyHandle,
                                         std::to_string(lowerBound) + "-" +
                                             std::to_string(upperBound - 1),
                                         url, &writeRangeToBufferDirect,
                                         isTrackingOverhead);
		    numRequests++;
                  }
		  curlMan.releaseEasyHandle(easyHandle);
                }
                auto resSpan =
                    boss::Span<int8_t>(bufferMan.buffer, bufferMan.totalSize, nullptr);

                return "ByteSequence"_(std::move(resSpan));
              } else {
                boss::ExpressionArguments args;
                for (size_t i = 0; i < dynamics.size(); i++) {
                  auto const &url = get<std::string>(dynamics[i]);
		  URLExistenceInfo existsInfo;

                  auto curlHandlesIt = curlHandlesMap.find(url);
                  if (curlHandlesIt == curlHandlesMap.end()) {
                    CURL *curl;
                    curl = curl_easy_init();
                    if (curl) {
		      existsInfo = checkURLExists(curl, url);
                      curlHandlesMap.emplace(url, curl);
                    } else {
                      throw std::runtime_error(
                          "Could not allocate resources for curl handle");
                    }
                  }
                  CURL *curl = curlHandlesMap[url];


                  auto bufferIt = bufferMap.find(url);
                  if (bufferIt == bufferMap.end()) {
		    auto const totalSize = getFileLength(url, curl);
                    bufferMap.emplace(url, Engine::BufferManager(totalSize, existsInfo.exists));
                    if (isTrackingOverhead && isTrackingRequired) {
                      overheadBufferMap.emplace(
                          url, Engine::DummyBufferManager(totalSize));
                    }
                  }
		  
                  auto &bufferMan = bufferMap[url];

		  bufferMan.totalRequested += existsInfo.requested;
		  bufferMan.totalHeadersDownloaded += existsInfo.headers;
		  bufferMan.totalPretransferTime += existsInfo.pretransferTime;
		  bufferMan.totalDownloadTime += existsInfo.downloadTime;
		  if (!bufferMan.exists) {
		    throw std::runtime_error("Remote resource does not exist.");
		  }
		  
                  std::vector<std::pair<int64_t, int64_t>> wholeBounds = {
                      {0, bufferMan.totalSize}};

                  if (isTrackingOverhead && isTrackingRequired) {
                    auto &dummyBufferMan = overheadBufferMap[url];
                    auto dummyBounds =
                        extractBoundPairs(wholeBounds, dummyBufferMan);
                    dummyBufferMan.addLoaded(dummyBounds);
                  }

                  auto bounds =
                      extractBoundPairs(wholeBounds, bufferMan, DEFAULT_PADDING,
                                        DEFAULT_ALIGNMENT, DEFAULT_MAX_RANGES, DEFAULT_MAX_REQUESTS);

                  if (isTrackingOverhead) {
                    bufferMan.addLoaded(bounds);
                  }

                  for (const auto &[lowerBound, upperBound] : bounds) {
                    bufferMan.currentOffset = lowerBound;
                    allocateRangeFromURL(bufferMan, curl,
                                         std::to_string(lowerBound) + "-" +
                                             std::to_string(upperBound - 1),
                                         url, &writeRangeToBuffer,
                                         isTrackingOverhead);
                  }

                  auto resSpan =
                      boss::Span<int8_t>(bufferMan.buffer, bufferMan.totalSize, nullptr);
                  auto resExpr = "ByteSequence"_(std::move(resSpan));
                  args.push_back(std::move(resExpr));

		  
		  curl_easy_cleanup(curl);
		  curlHandlesMap.erase(url);
                }

                return boss::ComplexExpression("ByteSequences"_, {},
                                               std::move(args), {});
              }
            } else if (head == "StartTrackingOverhead"_) {
	      if (dynamics.size() > 0) {
		isTrackingRequired =
		  std::holds_alternative<boss::Symbol>(dynamics[0]) &&
		  get<boss::Symbol>(dynamics[0]) == "TrackRequired"_;
	      }
              isTrackingOverhead = true;
              return "TrackingOverhead"_;
            } else if (head == "StopTrackingOverhead"_) {
	      isTrackingRequired = false;
              isTrackingOverhead = false;
              return "NotTrackingOverhead"_;
            } else if (head == "GetOverhead"_) {
              auto const &url = get<std::string>(dynamics[0]);
	      
	      auto totalRequired = -1;
	      if (isTrackingRequired) {
		auto overheadBufferIt = overheadBufferMap.find(url);
		if (overheadBufferIt == overheadBufferMap.end()) {
		  return "InvalidOverheadRequest"_();
		}
		auto &dummyBufferMan = overheadBufferMap[url];
		totalRequired = dummyBufferMan.totalRequired;
	      }
	      
              auto bufferIt = bufferMap.find(url);
              if (bufferIt == bufferMap.end()) {
                return "InvalidOverheadRequest"_();
              }
              auto &bufferMan = bufferMap[url];
	      
              return "Overhead"_(
                  "Size"_("Loaded"_(bufferMan.totalLoaded),
                          "Downloaded"_(bufferMan.totalDownloaded),
                          "Required"_(totalRequired),
                          "Requested"_(bufferMan.totalRequested),
                          "Headers"_(bufferMan.totalHeadersDownloaded)),
                  "Time"_("Pretransfer"_(bufferMan.totalPretransferTime),
                          "Download"_(bufferMan.totalDownloadTime)));
            } else if (head == "GetTotalOverhead"_) {
              int64_t totalLoaded = 0;
              int64_t totalDownloaded = 0;
              int64_t totalRequired = isTrackingRequired ? 0 : -1;
              int64_t totalRequested = 0;
              int64_t totalHeaders = 0;
              int64_t totalPretransferTime = 0;
              int64_t totalDownloadTime = 0;

              for (auto const &[url, bufferMan] : bufferMap) {
		if (isTrackingRequired) {
		  auto &dummyBufferMan = overheadBufferMap[url];
		  totalRequired += dummyBufferMan.totalRequired;
		}
                totalLoaded += bufferMan.totalLoaded;
                totalDownloaded += bufferMan.totalDownloaded;
                totalRequested += bufferMan.totalRequested;
                totalHeaders += bufferMan.totalHeadersDownloaded;
                totalPretransferTime += bufferMan.totalPretransferTime;
                totalDownloadTime += bufferMan.totalDownloadTime;
              }

              return "Overhead"_("Size"_("Loaded"_(totalLoaded),
                                         "Downloaded"_(totalDownloaded),
                                         "Required"_(totalRequired),
                                         "Requested"_(totalRequested),
                                         "Headers"_(totalHeaders)),
                                 "Time"_("Pretransfer"_(totalPretransferTime),
                                         "Download"_(totalDownloadTime)));

            } else if (head == "ClearCaches"_) {
	      for (auto &[url, curlHandle] : curlHandlesMap) {
		curl_easy_cleanup(curlHandle);
	      }
	      requestsManagersMap.clear();
	      curlHandlesMap.clear();
              bufferMap.clear();
              overheadBufferMap.clear();
              return "CachesCleared"_;
            } else if (head == "GetFileLength"_) {
	      auto const &url = get<std::string>(dynamics[0]);
	      CURL *curl;
	      curl = curl_easy_init();
	      if (curl) {
		auto const totalSize = getFileLength(url, curl);
		return (int64_t) totalSize;
	      } else {
		throw std::runtime_error("Could not allocate resources for curl handle");
	      }
            } else if (head == "GetEngineCapabilities"_) {
              return "List"_("Fetch"_, "StartTrackingOverhead"_,
                             "StopTrackingOverhead"_, "GetOverhead"_,
                             "GetTotalOverhead"_, "ClearCaches"_, "GetFileLength"_);
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

} // namespace boss::engines::RBL

static auto &enginePtr(bool initialise = true) {
  static std::mutex m;
  std::lock_guard const lock(m);
  static auto engine = std::unique_ptr<boss::engines::RBL::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::RBL::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
