
#include "config.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#pragma once

namespace boss::benchmarks::LazyLoading::utilities {

using namespace std;
using namespace boss;
  using boss::utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using boss::benchmarks::LazyLoading::config::paths::IN_MEM_CONV_CSV_SIZE_PATH;
using boss::benchmarks::LazyLoading::config::paths::IN_MEM_CONV_CSV_TIME_PATH;
using boss::benchmarks::LazyLoading::config::paths::OVERHEAD_CSV_SIZE_PATH;
using boss::benchmarks::LazyLoading::config::paths::OVERHEAD_CSV_TIME_PATH;
using boss::benchmarks::LazyLoading::config::paths::
    QUERY_OVERHEAD_CSV_SIZE_PATH;
using boss::benchmarks::LazyLoading::config::paths::
    QUERY_OVERHEAD_CSV_TIME_PATH;

inline void writeOverheadData(boss::ComplexExpression &&e, double selectivity) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();
  auto doubleToString = [&](double value) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(6) << value;
    return oss.str();
  };
  auto extractSizeAsCSV = [&](boss::ComplexExpression &&sizeExpr) {
    auto [sizeHead, sizeUnused_, sizeDynamics, sizeSpans] =
        std::move(sizeExpr).decompose();
    std::ostringstream data;
    data << doubleToString(selectivity) << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[1])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[2])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[3])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  auto extractTimeAsCSV = [&](boss::ComplexExpression &&timeExpr) {
    auto [timeHead, timeUnused_, timeDynamics, timeSpans] =
        std::move(timeExpr).decompose();
    std::ostringstream data;
    data << doubleToString(selectivity) << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[1])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  bool sizeExists = std::filesystem::exists(OVERHEAD_CSV_SIZE_PATH);
  bool timeExists = std::filesystem::exists(OVERHEAD_CSV_TIME_PATH);
  std::ofstream overheadSizeFile(OVERHEAD_CSV_SIZE_PATH,
                                 std::ios::out | std::ios::app);
  std::ofstream overheadTimeFile(OVERHEAD_CSV_TIME_PATH,
                                 std::ios::out | std::ios::app);
  if (overheadSizeFile.is_open() && overheadTimeFile.is_open()) {
    if (!sizeExists) {
      overheadSizeFile << "Selectivity,Loaded,Required,Requested,Headers\n";
    }
    if (!timeExists) {
      overheadTimeFile << "Selectivity,Pretransfer,Download\n";
    }

    overheadSizeFile << extractSizeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[0])));
    overheadTimeFile << extractTimeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[1])));

    overheadSizeFile.close();
    overheadTimeFile.close();
  } else {
    std::cerr << "Unable to open or create file: " << OVERHEAD_CSV_SIZE_PATH
              << " or " << OVERHEAD_CSV_TIME_PATH << std::endl;
  }
};

inline void writeInMemConvergenceData(boss::ComplexExpression &&e,
                                      int64_t numQueries,
                                      int64_t benchmarkState) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();
  auto extractSizeAsCSV = [&](boss::ComplexExpression &&sizeExpr) {
    auto [sizeHead, sizeUnused_, sizeDynamics, sizeSpans] =
        std::move(sizeExpr).decompose();
    std::ostringstream data;
    data << benchmarkState << ",";
    data << numQueries << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[1])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[2])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[3])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  auto extractTimeAsCSV = [&](boss::ComplexExpression &&timeExpr) {
    auto [timeHead, timeUnused_, timeDynamics, timeSpans] =
        std::move(timeExpr).decompose();
    std::ostringstream data;
    data << benchmarkState << ",";
    data << numQueries << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[1])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  bool sizeExists = std::filesystem::exists(IN_MEM_CONV_CSV_SIZE_PATH);
  bool timeExists = std::filesystem::exists(IN_MEM_CONV_CSV_TIME_PATH);
  std::ofstream overheadSizeFile(IN_MEM_CONV_CSV_SIZE_PATH,
                                 std::ios::out | std::ios::app);
  std::ofstream overheadTimeFile(IN_MEM_CONV_CSV_TIME_PATH,
                                 std::ios::out | std::ios::app);
  if (overheadSizeFile.is_open() && overheadTimeFile.is_open()) {
    if (!sizeExists) {
      overheadSizeFile
          << "BenchmarkState,NumQueries,Loaded,Required,Requested,Headers\n";
    }
    if (!timeExists) {
      overheadTimeFile << "BenchmarkState,NumQueries,Pretransfer,Download\n";
    }

    overheadSizeFile << extractSizeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[0])));
    overheadTimeFile << extractTimeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[1])));

    overheadSizeFile.close();
    overheadTimeFile.close();
  } else {
    std::cerr << "Unable to open or create file: " << OVERHEAD_CSV_SIZE_PATH
              << " or " << OVERHEAD_CSV_TIME_PATH << std::endl;
  }
};

inline void writeOverheadQueryData(boss::ComplexExpression &&e,
                                   int64_t systemKey) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();
  auto extractSizeAsCSV = [&](boss::ComplexExpression &&sizeExpr) {
    auto [sizeHead, sizeUnused_, sizeDynamics, sizeSpans] =
        std::move(sizeExpr).decompose();
    std::ostringstream data;
    data << systemKey << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[1])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[2])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[3])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  auto extractTimeAsCSV = [&](boss::ComplexExpression &&timeExpr) {
    auto [timeHead, timeUnused_, timeDynamics, timeSpans] =
        std::move(timeExpr).decompose();
    std::ostringstream data;
    data << systemKey << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[1])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  bool sizeExists = std::filesystem::exists(QUERY_OVERHEAD_CSV_SIZE_PATH);
  bool timeExists = std::filesystem::exists(QUERY_OVERHEAD_CSV_TIME_PATH);
  std::ofstream overheadSizeFile(IN_MEM_CONV_CSV_SIZE_PATH,
                                 std::ios::out | std::ios::app);
  std::ofstream overheadTimeFile(IN_MEM_CONV_CSV_TIME_PATH,
                                 std::ios::out | std::ios::app);
  if (overheadSizeFile.is_open() && overheadTimeFile.is_open()) {
    if (!sizeExists) {
      overheadSizeFile << "SystemKey,Loaded,Required,Requested,Headers\n";
    }
    if (!timeExists) {
      overheadTimeFile << "SystemKey,Pretransfer,Download\n";
    }

    overheadSizeFile << extractSizeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[0])));
    overheadTimeFile << extractTimeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[1])));

    overheadSizeFile.close();
    overheadTimeFile.close();
  } else {
    std::cerr << "Unable to open or create file: "
              << QUERY_OVERHEAD_CSV_SIZE_PATH << " or "
              << QUERY_OVERHEAD_CSV_TIME_PATH << std::endl;
  }
};

  inline void writeCURLData(boss::ComplexExpression &&e,
			    std::string &headerPreamble, std::string &dataPreamble, std::string &pathSize, std::string &pathTime) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();
  auto extractSizeAsCSV = [&](boss::ComplexExpression &&sizeExpr) {
    auto [sizeHead, sizeUnused_, sizeDynamics, sizeSpans] =
        std::move(sizeExpr).decompose();
    std::ostringstream data;
    data << dataPreamble;
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[1])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[2])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[3])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(sizeDynamics[4])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  auto extractTimeAsCSV = [&](boss::ComplexExpression &&timeExpr) {
    auto [timeHead, timeUnused_, timeDynamics, timeSpans] =
        std::move(timeExpr).decompose();
    std::ostringstream data;
    data << dataPreamble;
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[0])
                                  .getDynamicArguments()[0])
         << ",";
    data << std::get<int64_t>(std::get<boss::ComplexExpression>(timeDynamics[1])
                                  .getDynamicArguments()[0])
         << "\n";

    return data.str();
  };

  bool sizeExists = std::filesystem::exists(pathSize);
  bool timeExists = std::filesystem::exists(pathTime);
  std::ofstream overheadSizeFile(pathSize,
                                 std::ios::out | std::ios::app);
  std::ofstream overheadTimeFile(pathTime,
                                 std::ios::out | std::ios::app);
  if (overheadSizeFile.is_open() && overheadTimeFile.is_open()) {
    if (!sizeExists) {
      overheadSizeFile << headerPreamble << "Loaded,Downloaded,Required,Requested,Headers\n";
    }
    if (!timeExists) {
      overheadTimeFile << headerPreamble << "Pretransfer,Download\n";
    }

    overheadSizeFile << extractSizeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[0])));
    overheadTimeFile << extractTimeAsCSV(
        std::move(std::get<boss::ComplexExpression>(dynamics[1])));

    overheadSizeFile.close();
    overheadTimeFile.close();
  } else {
    std::cerr << "Unable to open or create file: "
              << pathSize << " or "
              << pathTime << std::endl;
  }
};
  
  inline boss::Expression wrapEval(boss::Expression &&expression, int32_t equalityRhs) {
    return std::move("EvaluateIf"_("Equals"_("STAGE"_, equalityRhs), std::move(expression)));
  };

} // namespace boss::benchmarks::LazyLoading::utilities
