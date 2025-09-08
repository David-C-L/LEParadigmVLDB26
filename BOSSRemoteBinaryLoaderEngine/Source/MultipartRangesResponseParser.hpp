#pragma once

#include <string_view>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <iostream>

class MultipartRangeParser {
public:
  MultipartRangeParser(const char* response, size_t responseSize,
		       int8_t* output, size_t outputSize,
		       std::string_view boundary)
    : response_(response, responseSize), output_(output), outputSize_(outputSize), boundary_(boundary) {
    if (boundary_.size() < 2 || boundary_.substr(0, 2) != "--") {
      staticBoundary_ = "--" + std::string(boundary_);
      boundary_ = staticBoundary_;
    }
#ifdef PARSERDEBUG
    std::cerr << "[Parser] Initialized with boundary: " << boundary_ << "\n";
#endif
  }

  void parseAndApply() {
#ifdef PARSERDEBUG
    std::cerr << "[Parser] Streaming parse of response (" << response_.size() << " bytes)\n";
#endif

    const char* data = response_.data();
    const char* ptr = data;
    const char* end = data + response_.size();

    constexpr std::string_view key = "Content-Range: bytes ";

    while (ptr + 3 < end) {
      if (ptr[0] == '-' && ptr[1] == '-' && ptr[2] == '*') {
	ptr += 3;

	if (ptr + 2 <= end && ptr[0] == '\r' && ptr[1] == '\n') {
	  ptr += 2;
	}

	if (ptr + 2 <= end && ptr[0] == '-' && ptr[1] == '-') {
	  break;
	}

	const char* headersEnd = nullptr;
	for (const char* p = ptr; p + 3 < end; ++p) {
	  if (p[0] == '\r' && p[1] == '\n' && p[2] == '\r' && p[3] == '\n') {
	    headersEnd = p;
	    break;
	  }
	}
	if (!headersEnd) break;
	const char* contentStart = headersEnd + 4;

        const char* rangeStart = nullptr;
	for (const char* p = ptr; p + key.size() <= headersEnd; ++p) {
	  if (std::memcmp(p, key.data(), key.size()) == 0) {
	    rangeStart = p + key.size();
	    break;
	  }
	}
	if (!rangeStart) continue;

        const char* dash = rangeStart;
	while (dash < headersEnd && *dash != '-') ++dash;
	if (dash == headersEnd) continue;

	const char* slash = dash + 1;
	while (slash < headersEnd && *slash != '/') ++slash;
	if (slash == headersEnd) continue;

	size_t start = fast_atou64({rangeStart, static_cast<size_t>(dash - rangeStart)});
	size_t endByte = fast_atou64({dash + 1, static_cast<size_t>(slash - dash - 1)});
	size_t len = endByte - start + 1;

	if (start >= outputSize_ || start + len > outputSize_ || contentStart + len > end) {
	  throw std::out_of_range("Range or content out of bounds");
	}

#ifdef PARSERDEBUG
	std::cerr << "[Parser] Writing range [" << start << "-" << endByte << "] ("
		  << len << " bytes)\n";
#endif
	std::memcpy(output_ + start, contentStart, len);

	ptr = contentStart + len;
      } else {
	++ptr;
      }
    }

#ifdef PARSERDEBUG
    std::cerr << "[Parser] Finished streaming parse.\n";
#endif
  }
  
private:
  std::string_view response_;
  int8_t* output_;
  size_t outputSize_;
  std::string_view boundary_;
  std::string staticBoundary_;

  inline static uint64_t fast_atou64(std::string_view s) {
    uint64_t result = 0;
    for (char c : s) {
      if (c < '0' || c > '9') break;
      result = result * 10 + (c - '0');
    }
    return result;
  }

};
