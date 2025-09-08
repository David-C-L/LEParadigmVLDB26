#include "ITTNotifySupport.hpp"
#include "MiniseedQueries.hpp"
#include "TPCHQueries.hpp"
#include "ParquetTPCHQueries.hpp"
#include "config.hpp"
#include "utilities.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <benchmark/benchmark.h>
#include <random>
#include <unordered_set>
#include <duckdb.hpp>

#include <string>
#include <sstream>
#include <thread>
#include <chrono>
#include <regex>
#include <atomic>

namespace {

VTuneAPIInterface vtune{"BOSS"};

using namespace std;
using namespace boss;
using utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using boss::benchmarks::LazyLoading::config::paths::coordinator;
using boss::benchmarks::LazyLoading::config::paths::DE_PATH;
using boss::benchmarks::LazyLoading::config::paths::RBL_PATH;
using boss::benchmarks::LazyLoading::config::paths::VELOX_PATH;
using boss::benchmarks::LazyLoading::config::paths::WD_PATH;
using boss::benchmarks::LazyLoading::config::paths::CE_PATH;
using boss::benchmarks::LazyLoading::config::paths::PR_PATH;
  
using boss::benchmarks::LazyLoading::config::paths::MINISEED_OVERHEAD_CSV_SIZE_PATH;
using boss::benchmarks::LazyLoading::config::paths::MINISEED_OVERHEAD_CSV_TIME_PATH;
using boss::benchmarks::LazyLoading::config::paths::TPCH_OVERHEAD_CSV_SIZE_PATH;
using boss::benchmarks::LazyLoading::config::paths::TPCH_OVERHEAD_CSV_TIME_PATH;

using boss::benchmarks::LazyLoading::MiniseedQueries::bossQueries;
using boss::benchmarks::LazyLoading::MiniseedQueries::bossRangesQueries;
using boss::benchmarks::LazyLoading::MiniseedQueries::bossCycleRangesQueries;
using boss::benchmarks::LazyLoading::MiniseedQueries::dataVaultsQueries;
using boss::benchmarks::LazyLoading::MiniseedQueries::dataVaultsColumnGranularityQueries;
  
using boss::benchmarks::LazyLoading::TPCHQueries::bossQueriesTPCH;
using boss::benchmarks::LazyLoading::TPCHQueries::bossCycleQueriesTPCH;
using boss::benchmarks::LazyLoading::TPCHQueries::dataVaultsQueriesTPCH;
using boss::benchmarks::LazyLoading::TPCHQueries::dataVaultsColumnGranularityQueriesTPCH;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF;
  // extern int64_t boss::benchmarks::LazyLoading::TPCHQueries::NUM_THREADS;
  
using boss::benchmarks::LazyLoading::ParquetTPCHQueries::bossParquetCycleQueriesTPCH;
using boss::benchmarks::LazyLoading::ParquetTPCHQueries::duckDBQueriesTPCH;
using boss::benchmarks::LazyLoading::ParquetTPCHQueries::runDuckDBQuery;

using boss::benchmarks::LazyLoading::utilities::writeInMemConvergenceData;
using boss::benchmarks::LazyLoading::utilities::writeOverheadData;
using boss::benchmarks::LazyLoading::utilities::writeOverheadQueryData;
using boss::benchmarks::LazyLoading::utilities::writeCURLData;

std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
  int64_t TOP_OF_RANGE_MAX = 100000;
  int64_t TOP_OF_RANGE = 100;
  
  int64_t MAX_FETCH_RANGE = 1048576;
  double IN_MEM_CONV_RAND_INTERVAL_SELECTIVITY = 0.005;
  int64_t OPTIMAL_FETCH_RANGE = 512;
  int64_t MAX_NUM_QUERIES = 8192;
  int64_t BENCHMARK_LAZY = 0;
  int64_t BENCHMARK_EAGER = 1;

int64_t NUM_OVERHEAD_INTS = 134217728;
  int64_t HALF_OVERHEAD_INTS = NUM_OVERHEAD_INTS / 2;
  int64_t QUARTER_OVERHEAD_INTS = NUM_OVERHEAD_INTS / 4;
  int64_t OVERHEAD_ITERATIONS = 3;

  int64_t BYTES_TOTAL = 1 * 1024 * 1024 * 1024;
  int64_t BYTE_DOWNLOAD_START = BYTES_TOTAL / 4;
  int64_t BYTE_DOWNLOAD_END = BYTE_DOWNLOAD_START * 3;
  int64_t BYTES_TO_DOWNLOAD = BYTE_DOWNLOAD_END - BYTE_DOWNLOAD_START;

  std::string EXPECTED_INT_BUFFER_OUTPUT_PATH = "/home/david/Documents/PhD/symbol-store/BOSSLazyLoadingCoordinatorEngine/expected_ranges_out.bin";
  std::string LARGE_INT_BUFFER_URL = "http://maru02.doc.res.ic.ac.uk/files/wisent_data_short/tpch_compressed_10000MB_lineitem.bin";
  
std::string OVERHEAD_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/overhead_table.bin";

std::unordered_map<std::string, std::vector<std::function<boss::Expression()>>>
    queryMap{{"BOSS", bossCycleRangesQueries}, {"DATA_VAULTS", dataVaultsColumnGranularityQueries}};
  
std::unordered_map<std::string, std::vector<std::function<boss::Expression(TPCH_SF)>>>
    queryMapTPCH{{"BOSS", bossCycleQueriesTPCH}, {"DATA_VAULTS", dataVaultsColumnGranularityQueriesTPCH}};

std::unordered_map<std::string, std::vector<std::function<boss::Expression(TPCH_SF)>>>
    queryMapTPCHFileFormat{{"WISENT", bossCycleQueriesTPCH}, {"PARQUET", bossParquetCycleQueriesTPCH}};

std::vector<std::string> systemKeys{"BOSS", "DATA_VAULTS"};

std::vector<std::string> fileFormatKeys{"WISENT", "PARQUET"};

boss::ComplexExpression getRBLEngineAsList() {
  return "List"_(RBL_PATH);
};
  
boss::ComplexExpression getEnginesAsList() {
  if (librariesToTest.empty()) {
    return "List"_(RBL_PATH, WD_PATH, DE_PATH, VELOX_PATH, CE_PATH);
  }
  return {"List"_,
          {},
          boss::ExpressionArguments(librariesToTest.begin(),
                                    librariesToTest.end())};
};
  
boss::ComplexExpression getEnginesAsListWithParquet() {
  if (librariesToTest.empty()) {
    return "List"_(RBL_PATH, PR_PATH, DE_PATH, VELOX_PATH, CE_PATH);
  }
  return {"List"_,
          {},
          boss::ExpressionArguments(librariesToTest.begin(),
                                    librariesToTest.end())};
};

} // namespace


std::atomic<double> totalReceived(0.0);
std::atomic<bool> stopMonitoring(false);

void monitorNethogs() {
  FILE *pipe = popen("sudo nethogs -v 2", "r");
  if (!pipe) {
    std::cerr << "Failed to start nethogs process!" << std::endl;
    return;
  }

  char buffer[256];
  std::string line;
  std::regex benchmarkRegex(".*Benchmarks.*RECEIVED: ([0-9.]+).*");

  while (!stopMonitoring) {
    if (fgets(buffer, sizeof(buffer), pipe)) {
      line = buffer;

      std::smatch match;
      if (std::regex_search(line, match, benchmarkRegex) && match.size() > 1) {
	double received = std::stod(match[1].str());
	totalReceived.store(received);
      }
    }
  }

  pclose(pipe);
}

static void BenchmarkQueriesTPCH(benchmark::State &state) {
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }

  auto const systemKey = state.range(0);
  auto const queryIdx = state.range(1);
  auto const sf = static_cast<TPCH_SF>(state.range(2));
  auto const numThreads = static_cast<int64_t>(state.range(3));
  auto const numRanges = static_cast<int64_t>(state.range(4));

  boss::benchmarks::LazyLoading::TPCHQueries::NUM_THREADS = numThreads;
  boss::benchmarks::LazyLoading::TPCHQueries::NUM_RANGES = numRanges;

  
  std::string curlHeadersPreamble = "System,Query,Scale,Iteration,";
  std::string curlDataInitialPreamble = std::to_string(systemKey) + "," + std::to_string(queryIdx) + "," + std::to_string(sf) + ",";
  auto const query = systemKey == 0 ? "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      std::move(queryMapTPCH[systemKeys[systemKey]][queryIdx](sf))) :
    "DelegateBootstrapping"_(
			     coordinator, getEnginesAsList(),
			     "NonLECycle"_, std::move(queryMapTPCH[systemKeys[systemKey]][queryIdx](sf)));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearTables"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));
  auto const clearWisentCachesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearWisentCaches"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_()));
  auto const setVeloxBatchQuery =
    "EvaluateInEngines"_(getEnginesAsList(), std::move("Set"_("internalBatchNumRows"_, static_cast<int>(4096))));


  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(resetCloned)));

  for (int i = 0; i < 2; i++) {
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto setVeloxBatchCloned =
        setVeloxBatchQuery.clone(expressions::CloneReason::FOR_TESTING);
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(setVeloxBatchCloned)));
    auto res = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    if(get<boss::ComplexExpression>(res).getHead() ==
       "ErrorWhenEvaluatingExpression"_) {
      std::stringstream output;
      output << res;
      state.SkipWithError(std::move(output).str());
    }
  }
  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesClonedFirst)));
  auto i = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto setVeloxBatchCloned =
        setVeloxBatchQuery.clone(expressions::CloneReason::FOR_TESTING);
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(setVeloxBatchCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(startTrackingCloned)));

    vtune.startSampling("TPCH - BOSS");
    state.ResumeTiming();
    auto res = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    state.PauseTiming();
    vtune.stopSampling();
    auto overheadData = boss::evaluate(std::move(getTrackingCloned));
    benchmark::DoNotOptimize(boss::evaluate(std::move(stopTrackingCloned)));
    std::string curlDataPreamble = curlDataInitialPreamble + std::to_string(i++) + ",";
    writeCURLData(
        std::move(std::get<boss::ComplexExpression>(overheadData)),
	curlHeadersPreamble, curlDataPreamble, TPCH_OVERHEAD_CSV_SIZE_PATH, TPCH_OVERHEAD_CSV_TIME_PATH
        );
  }
  auto resetCloned1 = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(resetCloned1)));
  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearBuffersClonedAgain =
      clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearWisentCachesClonedAgain =
      clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesClonedAgain)));
  benchmark::DoNotOptimize(boss::evaluate(std::move(clearBuffersClonedAgain)));
  benchmark::DoNotOptimize(boss::evaluate(std::move(clearWisentCachesClonedAgain)));
  benchmark::DoNotOptimize(boss::evaluate(std::move(resetClonedAgain)));
}

static void BenchmarkQueriesTPCHParquet(benchmark::State &state) {
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
      benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }
  vtune.stopSampling();
  
  auto const fileFormatKey = state.range(0);
  auto const queryIdx = state.range(1);
  auto const sf = static_cast<TPCH_SF>(state.range(2));
  auto const numThreads = static_cast<int64_t>(state.range(3));
  auto const numRanges = static_cast<int64_t>(state.range(4));
  auto const compression = static_cast<int64_t>(state.range(5));
  auto const numVeloxThreads = static_cast<int64_t>(state.range(6));
  auto const veloxBatchSize = static_cast<int64_t>(state.range(7));

  boss::benchmarks::LazyLoading::TPCHQueries::NUM_THREADS = numThreads;
  boss::benchmarks::LazyLoading::TPCHQueries::NUM_RANGES = numRanges;
  boss::benchmarks::LazyLoading::TPCHQueries::COMPRESSION = compression;
  boss::benchmarks::LazyLoading::ParquetTPCHQueries::PARQUET_COMPRESSION = compression;

  auto const getEngineList = fileFormatKey == 0 ? &getEnginesAsList : &getEnginesAsListWithParquet;
  std::string curlHeadersPreamble = "FileFormat,Query,Scale,Iteration,";
  std::string curlDataInitialPreamble = std::to_string(fileFormatKey) + "," + std::to_string(queryIdx) + "," + std::to_string(sf) + ",";
  auto const query = fileFormatKey == 0 ? "DelegateBootstrapping"_(
      coordinator, getEngineList(),
      std::move(queryMapTPCHFileFormat[fileFormatKeys[fileFormatKey]][queryIdx](sf))) :
    "DelegateBootstrapping"_(
			     coordinator, getEngineList(),
			     std::move(queryMapTPCHFileFormat[fileFormatKeys[fileFormatKey]][queryIdx](sf)));
  auto const resetQuery = "ResetEngines"_();
  auto const setVeloxThreadsQuery =
    "EvaluateInEngines"_(getEngineList(), std::move("Set"_("maxThreads"_, int(numVeloxThreads))));
  auto const setVeloxBatchQuery =
    "EvaluateInEngines"_(getEngineList(), std::move("Set"_("internalBatchNumRows"_, int(veloxBatchSize))));
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("ClearTables"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("ClearCaches"_()));
  auto const clearWisentCachesQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("ClearWisentCaches"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEngineList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEngineList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEngineList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_()));

  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(resetCloned)));

  for (int i = 0; i < 2; i++) {
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto setVeloxThreadsCloned =
        setVeloxThreadsQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto setVeloxBatchCloned =
        setVeloxBatchQuery.clone(expressions::CloneReason::FOR_TESTING);

    benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(startTrackingCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(setVeloxThreadsCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(setVeloxBatchCloned)));
    auto res = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    }
  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesClonedFirst)));
  auto i = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto setVeloxThreadsCloned =
        setVeloxThreadsQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto setVeloxBatchCloned =
        setVeloxBatchQuery.clone(expressions::CloneReason::FOR_TESTING);
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(startTrackingCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(setVeloxThreadsCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(setVeloxBatchCloned)));

    vtune.startSampling("TPCH - BOSS");
    state.ResumeTiming();
    auto res = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    state.PauseTiming();
    vtune.stopSampling();
    auto overheadData = boss::evaluate(std::move(getTrackingCloned));
    benchmark::DoNotOptimize(boss::evaluate(std::move(stopTrackingCloned)));
    std::string curlDataPreamble = curlDataInitialPreamble + std::to_string(i++) + ",";
    writeCURLData(
        std::move(std::get<boss::ComplexExpression>(overheadData)),
	curlHeadersPreamble, curlDataPreamble, TPCH_OVERHEAD_CSV_SIZE_PATH, TPCH_OVERHEAD_CSV_TIME_PATH
        );
    }

  for (int i = 0; i < 0; i++) {
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);

    benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(startTrackingCloned)));
    auto res = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
  }

  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearBuffersClonedAgain =
      clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesClonedAgain =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesClonedAgain)));
  benchmark::DoNotOptimize(boss::evaluate(std::move(clearBuffersClonedAgain)));
  benchmark::DoNotOptimize(boss::evaluate(std::move(clearWisentCachesClonedAgain)));
  benchmark::DoNotOptimize(boss::evaluate(std::move(resetClonedAgain)));
}

static void BenchmarkQueries(benchmark::State &state) {
  vtune.stopSampling();
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }

  auto const systemKey = state.range(0);
  auto const queryIdx = state.range(1);
  std::string curlHeadersPreamble = "System,Query,Iteration,";
  std::string curlDataInitialPreamble = std::to_string(systemKey) + "," + std::to_string(queryIdx) + ",";
  auto const query = systemKey == 0 ? "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      std::move(queryMap[systemKeys[systemKey]][queryIdx]())) :
    "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move(queryMap[systemKeys[systemKey]][queryIdx]()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearTables"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));
  auto const clearWisentCachesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearWisentCaches"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_()));
  
  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetCloned));

  for (int i = 0; i < 1; i++) {
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);

    boss::evaluate(std::move(clearTablesCloned));
    boss::evaluate(std::move(clearBuffersCloned));
    boss::evaluate(std::move(clearWisentCachesCloned));
    boss::evaluate(std::move(loadEnginesCloned));
    auto res = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    if(queryIdx != 7 && get<boss::ComplexExpression>(res).getHead() ==
       "ErrorWhenEvaluatingExpression"_) {
      std::stringstream output;
      output << res;
      state.SkipWithError(std::move(output).str());
    }
  }
  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(loadEnginesClonedFirst));
  auto i = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    boss::evaluate(std::move(loadEnginesCloned));
    boss::evaluate(std::move(clearTablesCloned));
    boss::evaluate(std::move(clearBuffersCloned));
    boss::evaluate(std::move(clearWisentCachesCloned));
    boss::evaluate(std::move(startTrackingCloned));
    state.ResumeTiming();
    vtune.startSampling("MINISEED - BOSS");
    auto res = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    vtune.stopSampling();
    state.PauseTiming();
    auto overheadData = boss::evaluate(std::move(getTrackingCloned));
    boss::evaluate(std::move(stopTrackingCloned));
    std::string curlDataPreamble = curlDataInitialPreamble + std::to_string(i++) + ",";
    writeCURLData(
        std::move(std::get<boss::ComplexExpression>(overheadData)),
	curlHeadersPreamble, curlDataPreamble, MINISEED_OVERHEAD_CSV_SIZE_PATH, MINISEED_OVERHEAD_CSV_TIME_PATH
        );
  }

  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearBuffersClonedAgain =
      clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearWisentCachesClonedAgain =
      clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(clearTablesClonedAgain));
  boss::evaluate(std::move(clearBuffersClonedAgain));
  boss::evaluate(std::move(clearWisentCachesClonedAgain));
  boss::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkQueriesOverhead(benchmark::State &state) {
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }

  auto const systemKey = state.range(0);
  auto const queryIdx = state.range(1);
  auto const query = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      std::move(queryMap[systemKeys[systemKey]][queryIdx]()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearTables"_()));
  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_(OVERHEAD_TABLE_URL)));

  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(resetCloned)));
    benchmark::DoNotOptimize(boss::evaluate(std::move(loadEnginesCloned)));
    boss::evaluate(std::move(startTrackingCloned));
    vtune.startSampling("FrameworkOverhead - BOSS");
    state.ResumeTiming();
    benchmark::DoNotOptimize(boss::evaluate(std::move(cloned)));
    state.PauseTiming();
    vtune.stopSampling();
    auto overheadData = boss::evaluate(std::move(getTrackingCloned));
    boss::evaluate(std::move(stopTrackingCloned));
    writeOverheadQueryData(
        std::move(std::get<boss::ComplexExpression>(overheadData)), systemKey);
    state.ResumeTiming();
  }

  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(boss::evaluate(std::move(clearTablesClonedAgain)));
  benchmark::DoNotOptimize(boss::evaluate(std::move(resetClonedAgain)));
}

static void BenchmarkFrameworkOverhead(benchmark::State &state) {
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));

  for (auto _ : state) {
    state.PauseTiming();
    auto query =
        "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
    for (auto i = 0; i < 8; i++) {
      query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                       std::move(queryMap[systemKeys[0]][i]()));
      auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
      auto resetCloned =
          resetQuery.clone(expressions::CloneReason::FOR_TESTING);
      auto loadEnginesCloned =
          loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
      boss::evaluate(std::move(resetCloned));
      boss::evaluate(std::move(loadEnginesCloned));
      vtune.startSampling("FrameworkOverhead - BOSS");
      state.ResumeTiming();
      auto result = boss::evaluate(std::move(cloned));
      benchmark::DoNotOptimize(result);
      state.PauseTiming();
      vtune.stopSampling();
    }
  }
}

static void BenchmarkOverhead(benchmark::State &state) {
  auto const selectivity = static_cast<int64_t>(state.range(0));
  auto realSelectivity = static_cast<double>(selectivity) / TOP_OF_RANGE_MAX;
  auto numSelected = static_cast<int64_t>(realSelectivity * NUM_OVERHEAD_INTS);

  auto getRandVec = [&](int64_t size, int64_t upperBound) {
    if (size == NUM_OVERHEAD_INTS) {
      std::vector<int64_t> indices(size);
      std::iota(indices.begin(), indices.end(), 0);
      return indices;
    }
    std::vector<int64_t> indices;
    std::unordered_set<int64_t> taken;

    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib(0, upperBound - 1);

    while (indices.size() < size) {
      int64_t num = distrib(gen);
      if (taken.insert(num).second) {
        indices.push_back(num);
      }
    }

    return indices;
  };

  auto getGather = [&]() {
    std::vector<int64_t> indices = getRandVec(numSelected, NUM_OVERHEAD_INTS);
    boss::ExpressionArguments args;
    args.push_back(OVERHEAD_TABLE_URL);
    args.push_back(RBL_PATH);
    args.push_back("List"_(
        "List"_(boss::Span<int64_t const>(std::move(vector(indices))))));
    args.push_back("List"_("value1"_));

    auto gather = boss::ComplexExpression{"Gather"_, {}, std::move(args), {}};
    return std::move(gather);
  };

  auto const query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                              std::move(getGather()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery =
      "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                               "NonLECycle"_, std::move("GetOverhead"_(OVERHEAD_TABLE_URL)));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));

  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetCloned));
    
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    boss::evaluate(std::move(clearBuffersCloned));
    boss::evaluate(std::move(loadEnginesCloned));
    boss::evaluate(std::move(startTrackingCloned));
    state.ResumeTiming();
    auto result = boss::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(result);
    state.PauseTiming();
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto overheadData = boss::evaluate(std::move(getTrackingCloned));
    boss::evaluate(std::move(stopTrackingCloned));
    writeOverheadData(
        std::move(std::get<boss::ComplexExpression>(overheadData)),
        realSelectivity);
    state.ResumeTiming();
  }

  auto resetClonedAgain = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkRanges(benchmark::State &state) {
  auto const numRanges = static_cast<int64_t>(state.range(0));
  auto const numRequests = static_cast<int64_t>(state.range(1));
  int64_t chunkToGather = HALF_OVERHEAD_INTS / numRanges;

  auto getRangedVecs = [&]() {
    std::vector<int64_t> starts;
    std::vector<int64_t> ends;
    starts.reserve(numRanges);
    ends.reserve(numRanges);

    for (auto i = 0; i < NUM_OVERHEAD_INTS; i += 2 * chunkToGather) {
      starts.push_back(i);
      ends.push_back(i + chunkToGather);
    }

    return std::make_pair(std::move(starts), std::move(ends));
  };

  auto convertRangedVecsToByteRanges = [&]() {
    auto const &[starts, ends] = getRangedVecs();
    std::vector<int64_t> ranges;
    ranges.reserve(starts.size() * 2);

    for (size_t i = 0; i < starts.size(); i++) {
      ranges.emplace_back(starts[i] * 8);
      ranges.emplace_back(ends[i] * 8);
    }
    return ranges;
  };

  auto getFetchExpression = [&]() {
    auto ranges = convertRangedVecsToByteRanges();
    boss::ExpressionArguments args;
    args.push_back(std::move("List"_(std::move(boss::Span<int64_t>(ranges)))));
    args.push_back("http://maru02.doc.res.ic.ac.uk/files/wisent_data_short/tpch_10000MB_lineitem.bin");
    args.push_back(0);
    args.push_back(1);
    args.push_back(numRanges);
    args.push_back(-1);
    args.push_back(false);
    args.push_back(1);
    return boss::ComplexExpression("Fetch"_, {}, std::move(args), {});
  };

  auto getRangedGather = [&]() {
    auto const &[starts, ends] = getRangedVecs();
    boss::ExpressionArguments args;
    args.push_back(OVERHEAD_TABLE_URL);
    args.push_back(RBL_PATH);
    args.push_back("starts"_);
    args.push_back("ends"_);
    args.push_back("Table"_(
        "starts"_(
            "List"_(boss::Span<int64_t const>(std::move(vector(starts))))),
        "ends"_("List"_(boss::Span<int64_t const>(std::move(vector(ends)))))));
    args.push_back("List"_("value1"_));
    args.push_back(numRanges);

    auto gatherRanges =
        boss::ComplexExpression{"GatherRanges"_, {}, std::move(args), {}};
    return std::move(gatherRanges);
  };

  auto const query = "EvaluateInEngines"_(getRBLEngineAsList(),
                                              std::move(getFetchExpression()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));

  
  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetCloned));
  
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    boss::evaluate(std::move(clearBuffersCloned));
    boss::evaluate(std::move(loadEnginesCloned));
    state.ResumeTiming();
    if (numRequests <= numRanges) {
      auto result = boss::evaluate(std::move(cloned));
      benchmark::DoNotOptimize(result);
    }
  }

  auto resetClonedAgain = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkRangesCurlOnly(benchmark::State &state) {
  auto const numRanges = static_cast<int64_t>(state.range(0));
  auto const numRequests = static_cast<int64_t>(state.range(1));
  int64_t chunkToGather = BYTES_TO_DOWNLOAD / numRanges;

  auto getBounds = [&]() {
    std::vector<int64_t> bounds;
    bounds.reserve(numRanges * 2);

    for (int64_t i = BYTE_DOWNLOAD_START; i < BYTE_DOWNLOAD_END * 2; i += 2 * chunkToGather) {
      bounds.push_back(i);
      bounds.push_back(i + chunkToGather + 1);
    }

    return bounds;
  };

  auto getFetch = [&]() {
    auto bounds = getBounds();
    int64_t padding = 0;
    int64_t alignment = 1;
    int64_t ranges = numRanges / numRequests;
    int64_t requests = -1;
    int64_t numThreads = 1;
    bool trackingCache = false;
    boss::ExpressionArguments args;
    args.push_back(std::move("List"_(std::move(boss::Span<int64_t>(std::move(bounds))))));
    args.push_back("http://maru02.doc.res.ic.ac.uk/files/wisent_data_short/tpch_10000MB_lineitem.bin");
    args.push_back(padding);
    args.push_back(alignment);
    args.push_back(ranges);
    args.push_back(requests);
    args.push_back(trackingCache);
    args.push_back(numThreads);

    return boss::ComplexExpression("Fetch"_, {}, std::move(args), {});
  };

  auto const query = "EvaluateInEngines"_(getRBLEngineAsList(), getFetch());
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getRBLEngineAsList(), std::move("HELLO :)"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getRBLEngineAsList(), std::move("ClearCaches"_()));

  
  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetCloned));
  
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto cloned1 = query.clone(expressions::CloneReason::FOR_TESTING);
    auto cloned2 = query.clone(expressions::CloneReason::FOR_TESTING);
    auto cloned3 = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    boss::evaluate(std::move(clearBuffersCloned));
    boss::evaluate(std::move(loadEnginesCloned));
    state.ResumeTiming();
    if (numRequests <= numRanges) {
      auto result = boss::evaluate(std::move(cloned));
      auto result1 = boss::evaluate(std::move(cloned1));
      auto result2 = boss::evaluate(std::move(cloned2));
      benchmark::DoNotOptimize(result);
      benchmark::DoNotOptimize(result1);
      benchmark::DoNotOptimize(result2);
    }
    state.PauseTiming();
    state.ResumeTiming();
  }

  auto resetClonedAgain = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkInMemConvergence(benchmark::State &state) {
  auto const benchmarkState = static_cast<int64_t>(state.range(0));
  auto const numQueries = static_cast<int64_t>(state.range(1));

  auto getRandRange = [&](int64_t upperBound) {
    int64_t intervalSize = IN_MEM_CONV_RAND_INTERVAL_SELECTIVITY * upperBound;

    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib(0, upperBound - 1);
    int64_t start = distrib(gen);
    int64_t end = upperBound - 1 - start < intervalSize ? upperBound
                                                        : start + intervalSize;

    std::vector<int64_t> starts;
    std::vector<int64_t> ends;
    starts.push_back(start);
    ends.push_back(end);

    return std::make_pair(std::move(starts), std::move(ends));
  };

  auto getRandRangedGather = [&]() {
    auto const &[starts, ends] = getRandRange(NUM_OVERHEAD_INTS);
    boss::ExpressionArguments args;
    args.push_back(OVERHEAD_TABLE_URL);
    args.push_back(RBL_PATH);
    args.push_back("starts"_);
    args.push_back("ends"_);
    args.push_back("Table"_(
        "starts"_(
            "List"_(boss::Span<int64_t const>(std::move(vector(starts))))),
        "ends"_("List"_(boss::Span<int64_t const>(std::move(vector(ends)))))));
    args.push_back("List"_("value1"_));
    args.push_back(OPTIMAL_FETCH_RANGE);

    auto gatherRanges = boss::ComplexExpression{"GatherRanges"_, {}, std::move(args), {}};
    auto project = "Project"_(std::move(gatherRanges), "As"_("value1"_, "value1"_));
    return std::move(project);
  };

  auto getRandSelect = [&](boss::Expression &&table) {
    auto const &[starts, ends] = getRandRange(NUM_OVERHEAD_INTS);
    auto start = starts[0];
    auto end = ends[0];

    auto select = "Select"_(std::move(table),
                            "Where"_("And"_("Greater"_("value1"_, start),
                                            "Greater"_(end, "value1"_))));

    return std::move(select);
  };

  auto getParseTables = [&]() {
    std::vector<std::string> overheadURLFiles = {OVERHEAD_TABLE_URL};
    auto parseTables = "ParseTables"_(
        RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                      std::move(vector(overheadURLFiles)))))));
    return std::move(parseTables);
  };

  auto getOverheadTable = [&]() {
    auto query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                          std::move(getParseTables()));
    auto table = boss::evaluate(std::move(query));
    return std::move(table);
  };
  
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery =
      "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                               "NonLECycle"_, std::move("GetOverhead"_(OVERHEAD_TABLE_URL)));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));

  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetCloned));

  for (auto _ : state) {
    state.PauseTiming();
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    boss::evaluate(std::move(clearBuffersCloned));
    boss::evaluate(std::move(loadEnginesCloned));
    boss::evaluate(std::move(startTrackingCloned));
    if (benchmarkState == BENCHMARK_LAZY) {
      state.ResumeTiming();
      for (auto i = 0; i < numQueries; i++) {
        state.PauseTiming();
        auto query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                              std::move(getRandRangedGather()));
        state.ResumeTiming();
        auto result = boss::evaluate(std::move(query));
        benchmark::DoNotOptimize(result);
      }
    } else if (benchmarkState == BENCHMARK_EAGER) {
      auto overheadTable = getOverheadTable();
      state.ResumeTiming();
      for (auto i = 0; i < numQueries; i++) {
        state.PauseTiming();
        auto overheadTableClone =
            overheadTable.clone(expressions::CloneReason::FOR_TESTING);
        auto query = "DelegateBootstrapping"_(
            coordinator, getEnginesAsList(),
            "NonLECycle"_, std::move(getRandSelect(std::move(overheadTableClone))));
        state.ResumeTiming();
        auto result = boss::evaluate(std::move(query));
        benchmark::DoNotOptimize(result);
      }
    }
    state.PauseTiming();
    auto overheadData = boss::evaluate(std::move(getTrackingCloned));
    boss::evaluate(std::move(stopTrackingCloned));
    writeInMemConvergenceData(
        std::move(std::get<boss::ComplexExpression>(overheadData)), numQueries,
        benchmarkState);
    state.ResumeTiming();
  }

  auto resetClonedAgain = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  boss::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkQueriesTPCHDuckDBParquet(benchmark::State &state) {
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }
  vtune.stopSampling();

  duckdb::DBConfig config;
  config.options.maximum_threads = 3;
  boss::benchmarks::LazyLoading::ParquetTPCHQueries::PARQUET_COMPRESSION = 2;


  auto const queryIdx = state.range(0);
  auto const sf = static_cast<TPCH_SF>(state.range(1));

  auto query = duckDBQueriesTPCH[queryIdx](sf);
  auto i = 0;
  for (auto _ : state) {
    state.PauseTiming();
    duckdb::DuckDB db(nullptr, &config);
    duckdb::Connection con(db);
    con.Query("INSTALL httpfs;");
    con.Query("LOAD httpfs;");

    state.ResumeTiming();
    auto res = runDuckDBQuery(con, query);
    benchmark::DoNotOptimize(res);
    state.PauseTiming();
  }
}

static void TPCHArguments(benchmark::internal::Benchmark* b) {
  std::vector<int> sysKeys = {1};
  std::vector<int> queryIdxs = {2};
  std::vector<int> sfs = {4};

  std::vector<int> numThreads = {1};
  std::vector<int> numRanges = {1048576};
  for (auto sysKey : sysKeys) {
    for (auto queryIdx : queryIdxs) {
      for (auto sf : sfs) {
	for (auto nr : numRanges) {
	  for (auto nt : numThreads) {
	    b->Args({sysKey, queryIdx, sf, nt, nr});
	  }
	}
      }
    }
  }
}

static void TPCHDuckDBParquetArguments(benchmark::internal::Benchmark* b) {
  std::vector<int> queryIdxs = {2};
  std::vector<int> sfs = {6};

  for (auto queryIdx : queryIdxs) {
    for (auto sf : sfs) {
      std::string name = "Query" + std::to_string(queryIdx) + "_SF" + std::to_string(sf);
      b->Args({queryIdx, sf})->Name(name);
    }
  }
}

static void TPCHParquetArguments(benchmark::internal::Benchmark* b) {
  std::vector<int> fileFormatKeys = {0};
  std::vector<int> queryIdxs = {2};
  std::vector<int> sfs = {4};
  std::vector<int> numThreads = {1};
  std::vector<int> compression = {0};
  std::vector<int> numVeloxThreads = {1};
  std::vector<int> veloxBatchSizes = {4096};
  
  std::vector<int> numRanges = {1048576};
  
  for (auto fileFormatKey : fileFormatKeys) {
    for (auto queryIdx : queryIdxs) {
      for (auto sf : sfs) {
	for (auto nr : numRanges) {
	  for (auto nt : numThreads) {
	    for (auto c : compression) {
	      for (auto vt : numVeloxThreads) {
		for (auto vb : veloxBatchSizes) {
		  std::string name = "TPCHQuery" + std::to_string(queryIdx) + "_SF" + std::to_string(sf) + "_C" + std::to_string(c) + "_" + (fileFormatKey == 0 ? "WISENT" : "PARQUET");
		  b->Args({fileFormatKey, queryIdx, sf, nt, nr, c, vt, vb});
		}
	      }
	    }
	  }
	}
      }
    }
  }
}


BENCHMARK(BenchmarkQueriesTPCH)
    ->Unit(benchmark::kMillisecond)
    ->Apply(TPCHArguments)
    ->Iterations(3);

BENCHMARK(BenchmarkQueriesTPCHDuckDBParquet)
    ->Unit(benchmark::kMillisecond)
    ->Apply(TPCHDuckDBParquetArguments)
    ->Iterations(3);

BENCHMARK(BenchmarkQueriesTPCHParquet)
    ->Unit(benchmark::kMillisecond)
    ->Apply(TPCHParquetArguments)
    ->Iterations(3);

BENCHMARK(BenchmarkQueries)
->Unit(benchmark::kMillisecond)
->ArgsProduct({{1},
	       {0,1,2,3,4,5,6,7}})
->Iterations(3);

BENCHMARK(BenchmarkQueriesOverhead)
->Unit(benchmark::kMillisecond)
->ArgsProduct({{0,1},
	       {6}})
->Iterations(3);

BENCHMARK(BenchmarkFrameworkOverhead)
->Unit(benchmark::kMillisecond)
->Iterations(3);

BENCHMARK(BenchmarkRanges)
->Unit(benchmark::kMillisecond)
->ArgsProduct({benchmark::CreateRange(1, 1048576, 4), benchmark::CreateRange(1, 1024, 2)})
->Iterations(3);

BENCHMARK(BenchmarkRangesCurlOnly)
->Unit(benchmark::kMillisecond)
->ArgsProduct({benchmark::CreateRange(1, 1048576, 4), benchmark::CreateRange(1, 1024, 2)})
->Iterations(3);

BENCHMARK(BenchmarkInMemConvergence)
->Unit(benchmark::kMillisecond)
->ArgsProduct({{0, 1}, benchmark::CreateRange(1, MAX_NUM_QUERIES, 2)})
->Iterations(3);

BENCHMARK(BenchmarkOverhead)
->Unit(benchmark::kMillisecond)
->RangeMultiplier(10)->Range(1, TOP_OF_RANGE_MAX)
->Iterations(3);


BENCHMARK_MAIN();


