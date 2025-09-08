#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <set>
#include <unordered_map>
#include <utility>
#include <iostream>
#include <filesystem>
#include <boost/dynamic_bitset.hpp>
#ifndef _WIN32
#include <dlfcn.h>
#else
#define NOMINMAX // max macro in minwindef.h interfering with std::max...
#include <windows.h>
constexpr static int RTLD_NOW = 0;
constexpr static int RTLD_NODELETE = 0;
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

namespace boss::engines::ParquetReader {

using EvalFunction = BOSSExpression *(*)(BOSSExpression *);
  
class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

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

  std::unordered_map<std::string, boost::dynamic_bitset<uint8_t>> currColChunksMap;

};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::ParquetReader
