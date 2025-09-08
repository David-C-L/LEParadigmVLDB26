#include <string_view>
#define CATCH_CONFIG_RUNNER
#include "../Source/BOSSLazyLoadingCoordinatorEngine.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <Serialization.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <fstream>
#include <numeric>
#include <typeinfo>
#include <variant>
#ifndef _WIN32
#include <dlfcn.h>
#else
#include <filesystem>
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

#include <algorithm>
#include <iterator>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <variant>

using boss::Expression;
using std::string;
using std::literals::string_literals::
operator""s;                        // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using boss::utilities::operator""_; // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;
using std::vector;
using namespace Catch::Matchers;
using boss::expressions::CloneReason;
using boss::expressions::ComplexExpression;
using boss::expressions::generic::get;
using boss::expressions::generic::get_if;
using boss::expressions::generic::holds_alternative;
namespace boss {
using boss::expressions::atoms::Span;
};
using std::int64_t;
using SerializationExpression = boss::serialization::Expression;

namespace {
std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

int64_t TIME_2019_08_02_02_00 = 1564711200000000000;
int64_t TIME_2019_08_02_02_02 = 1564711320000000000;
int64_t TIME_2019_08_02_03_00 = 1564714800000000000;
int64_t TIME_2019_08_02_14_00 = 1564754400000000000;
std::string DENORMALISED_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/denormalised_table_index.bin";
std::string FILES_TABLE_URL = "https://www.doc.ic.ac.uk/~dcl19/files_table.bin";
std::string CATALOG_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/catalog_table.bin";
std::string RANGED_FILES_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/rangedFilesTable.bin";
std::string RANGED_CATALOG_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/rangedCatalogTable.bin";
std::string RANGED_DATA_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/rangedDataTable.bin";
std::string RBL_PATH =
    "/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/"
    "debugBuild/libBOSSRemoteBinaryLoaderEngine.so";

std::unordered_map<std::string, std::function<boss::Expression()>> queryMap = {
    {"SIMPLE_SELECT_BOSS",
     [] {
       auto indicesGatherExpr =
           "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, "List"_("List"_()),
                     "List"_("network"_, "channel"_, "start_time"_));
       auto encodeIndices = "EncodeTable"_(std::move(indicesGatherExpr));
       auto innerIndicesProject =
           "Project"_(std::move(encodeIndices),
                      "As"_("__internal_indices_"_, "__internal_indices_"_,
                            "network"_, "network"_, "channel"_, "channel"_,
                            "start_time"_, "start_time"_));
       auto indicesProject = "Project"_(
           "Select"_(std::move(innerIndicesProject),
                     "Where"_("And"_(
                         "Equal"_("network"_, 0), "Equal"_("channel"_, 0),
                         "Greater"_("start_time"_, TIME_2019_08_02_02_00),
                         "Greater"_(TIME_2019_08_02_14_00, "start_time"_)))),
           "As"_("__internal_indices_"_, "__internal_indices_"_));
       auto outerGather = "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                                    std::move(indicesProject),
                                    "List"_("sample_value"_, "station"_));
       auto encodeOuterGather = "EncodeTable"_(std::move(outerGather));
       auto groupBy =
           "Group"_("Project"_(std::move(encodeOuterGather),
                               "As"_("sample_value"_, "sample_value"_,
                                     "station"_, "station"_)),
                    "By"_("station"_),
                    "As"_("avg_sample_value"_, "Avg"_("sample_value"_)));
       auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));
       return std::move(decodeGroupBy);
     }},
    {"RANGED_SELECT_BOSS",
     [] {
      auto filesGather =
          "Gather"_(RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("network"_, "channel"_, "station"_, "f_start"_,
                            "f_end"_, "f_file_key"_));
      auto catalogGather =
          "Gather"_(RANGED_CATALOG_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("start_time"_, "c_start"_, "c_end"_, "c_file_key"_,
                            "c_seq_no"_));

      auto encodeFiles = "EncodeTable"_(std::move(filesGather));

      auto filesSelect = "Select"_(
          std::move(encodeFiles),
          "Where"_("And"_("Equal"_("network"_, 0), "Equal"_("channel"_, 0))));
      auto catalogSelect = "Select"_(
          std::move(catalogGather),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "start_time"_))));
      auto filesCatalogJoin = "Project"_(
          "Join"_(std::move(catalogSelect), std::move(filesSelect),
                  "Where"_("Equal"_("c_file_key"_, "f_file_key"_))),
          "As"_("c_file_key"_, "c_file_key"_, "c_start"_, "c_start"_, "c_end"_,
                "c_end"_, "c_seq_no"_, "c_seq_no"_, "station"_, "station"_));

      auto saveJoin =
          "SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_);

      auto rangesGather = "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("c_start"_),
          std::move("c_end"_), std::move(saveJoin),
          std::move("List"_("d_seq_no"_, "d_file_key"_, "sample_value"_)));

      auto getJoin = "GetTable"_("FilesCatalogTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(rangesGather), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_key"_, "d_seq_no"_),
                                    "List"_("c_file_key"_, "c_seq_no"_))));

      auto dataFilesCatalogProject = "Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("station"_, "station"_, "sample_value"_, "sample_value"_));

      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "By"_("station"_),
                   "As"_("sum_sample_value"_, "Sum"_("sample_value"_)));
      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    }},
    {"STAGED_RANGED_SELECT_BOSS",
     [] {
      auto filesGather =
	"EvaluateIf"_("Equals"_("STAGE"_, 0), "Gather"_(RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("network"_, "channel"_, "station"_, "f_start"_,
                            "f_end"_, "f_file_key"_)));
      auto catalogGather =
	"EvaluateIf"_("Equals"_("STAGE"_, 1), "Gather"_(RANGED_CATALOG_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("start_time"_, "c_start"_, "c_end"_, "c_file_key"_,
                            "c_seq_no"_)));

      auto encodeFiles = "EvaluateIf"_("Equals"_("STAGE"_, 0), "EncodeTable"_(std::move(filesGather)));

      auto filesSelect = "EvaluateIf"_("Equals"_("STAGE"_, 0), "Select"_(
          std::move(encodeFiles),
          "Where"_("And"_("Equal"_("network"_, 0), "Equal"_("channel"_, 0)))));
      auto catalogSelect = "EvaluateIf"_("Equals"_("STAGE"_, 1), "Select"_(
          std::move(catalogGather),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "start_time"_)))));
      auto filesCatalogJoin = "EvaluateIf"_("Equals"_("STAGE"_, 1), "Project"_(
									       "EvaluateIf"_("Equals"_("STAGE"_, 1), "Join"_(std::move(catalogSelect), std::move(filesSelect),
															     "Where"_("Equal"_("c_file_key"_, "f_file_key"_)))),
          "As"_("c_file_key"_, "c_file_key"_, "c_start"_, "c_start"_, "c_end"_,
                "c_end"_, "c_seq_no"_, "c_seq_no"_, "station"_, "station"_)));

      auto saveJoin =
	"EvaluateIf"_("Equals"_("STAGE"_, 2), "SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_));

      auto rangesGather = "EvaluateIf"_("Equals"_("STAGE"_, 3), "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("c_start"_),
          std::move("c_end"_), std::move(saveJoin),
          std::move("List"_("d_seq_no"_, "d_file_key"_, "sample_value"_))));

      auto getJoin = "EvaluateIf"_("Equals"_("STAGE"_, 4), "GetTable"_("FilesCatalogTable"_));

      auto dataFilesCatalogJoin =
	"EvaluateIf"_("Equals"_("STAGE"_, 4), "Join"_(std::move(rangesGather), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_key"_, "d_seq_no"_),
                                    "List"_("c_file_key"_, "c_seq_no"_)))));

      auto dataFilesCatalogProject = "EvaluateIf"_("Equals"_("STAGE"_, 4), "Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("station"_, "station"_, "sample_value"_, "sample_value"_)));

      auto groupBy =
	"EvaluateIf"_("Equals"_("STAGE"_, 4), "Group"_(std::move(dataFilesCatalogProject), "By"_("station"_),
						       "As"_("sum_sample_value"_, "Sum"_("sample_value"_))));
      auto decodeGroupBy = "EvaluateIf"_("Equals"_("STAGE"_, 5), "DecodeTable"_(std::move(groupBy)));

      return std::move(decodeGroupBy);
     }},
    {"DV_SELECT",
     [] {
      std::vector<std::string> initialURLFiles = {FILES_TABLE_URL};
      std::vector<std::string> initialURLCatalog = {CATALOG_TABLE_URL};
      auto eagerLoadFilesExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLFiles)))))));
      auto eagerLoadCatalogExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLCatalog)))))));

      auto encodeFilesExpr = "EncodeTable"_(std::move(eagerLoadFilesExpr));
      auto encodeCatalogExpr = "EncodeTable"_(std::move(eagerLoadCatalogExpr));

      auto innerFilesSelect = "Select"_(
          std::move(encodeFilesExpr),
          "Where"_("And"_("Equal"_("network"_, 0), "Equal"_("channel"_, 0))));
      auto innerCatalogSelect = "Project"_("Select"_(
          std::move(encodeCatalogExpr),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "start_time"_)))),"As"_("c_file_location"_,"c_file_location"_,"c_seq_no"_,"c_seq_no"_));
      auto filesCatalogJoin = "Project"_(
          "Join"_(std::move(innerFilesSelect), std::move(innerCatalogSelect),
                  "Where"_("Equal"_("f_file_location"_, "c_file_location"_))),
          "As"_("c_file_location"_, "c_file_location"_, "c_seq_no"_,
                "c_seq_no"_, "station"_, "station"_));

      auto saveJoin =
          "SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveJoin));
      auto lazyLoadDataExpr =
          "ParseTables"_(RBL_PATH, std::move("c_file_location"_),
                         std::move(fileLocationDecoded));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto getJoin = "GetTable"_("FilesCatalogTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(encodeLazyLoadData), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_location"_, "d_seq_no"_),
                                    "List"_("c_file_location"_, "c_seq_no"_))));

      auto dataFilesCatalogProject = "Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("station"_, "station"_, "sample_value"_, "sample_value"_));

      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "By"_("station"_),
                   "As"_("sum_sample_value"_, "Sum"_("sample_value"_)));
      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    }}};
} // namespace
// NOLINTBEGIN(readability-magic-numbers)
// NOLINTBEGIN(bugprone-exception-escape)
// NOLINTBEGIN(readability-function-cognitive-complexity)

TEST_CASE("EvaluateInEnginesTest", "[]") { // NOLINT

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
    LibraryCache(LibraryCache &&) = delete;
    LibraryCache &operator=(LibraryCache const &) = delete;
    LibraryCache &operator=(LibraryCache &&) = delete;
  } libraries;
  boss::engines::LazyLoadingCoordinator::Engine engine;
  auto delegatedEngineSymbols = ::std::vector<int64_t>();
  std::vector<std::string> enginePaths;
  enginePaths.push_back(
      "/home/david/Documents/PhD/symbol-store/BOSSWisentDeserialiserEngine/"
      "debugBuild/libBOSSWisentDeserialiserEngine.so");
  enginePaths.push_back(
      "/home/david/Documents/PhD/symbol-store/BOSSConditionalEvaluationEngine/"
      "build/libBOSSConditionalEvaluationEngine.so");
  enginePaths.push_back(RBL_PATH);
  enginePaths.push_back("/home/david/Documents/PhD/symbol-store/"
                        "BOSSVeloxEngine/radixBuild/libBOSSVeloxEngine.so");
  enginePaths.push_back(
      "/home/david/Documents/PhD/symbol-store/BOSSDictionaryEncoderEngine/"
      "debugBuild/libBOSSDictionaryEncoderEngine.so");
  ::std::for_each(enginePaths.begin(), enginePaths.end(),
                  [&delegatedEngineSymbols, &libraries](auto &&enginePath) {
                    delegatedEngineSymbols.push_back(reinterpret_cast<int64_t>(
                        libraries.at(enginePath).evaluateFunction));
                  });
  auto newArgs = boss::ExpressionArguments();
  auto newSpanArgs = boss::expressions::ExpressionSpanArguments();
  newArgs.emplace_back(std::move("NonLECycle"_));
  newArgs.emplace_back(std::move(queryMap.at("RANGED_SELECT_BOSS")()));
  newSpanArgs.emplace_back(
      boss::Span<int64_t>(::std::move(delegatedEngineSymbols)));
  auto *toEval = new BOSSExpression{ComplexExpression(
      "EvaluateInEngines"_, {}, std::move(newArgs), std::move(newSpanArgs))};
  auto *r = new BOSSExpression{engine.evaluate(std::move(toEval->delegate))};
  delete toEval;

  auto res = std::move(r->delegate);
  delete r;

  std::cout << res << std::endl;
}

int main(int argc, char *argv[]) {
  Catch::Session session;
  session.cli(session.cli() |
              Catch::clara::Opt(librariesToTest, "library")["--library"]);
  auto const returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) {
    return returnCode;
  }
  return session.run();
}
