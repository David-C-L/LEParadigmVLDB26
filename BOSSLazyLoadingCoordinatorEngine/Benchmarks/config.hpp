#include <string>
#pragma once

namespace boss::benchmarks::LazyLoading::config::paths {
inline std::string RBL_PATH =
    "/data/david/BOSSRemoteBinaryLoaderEngine/"
    "release/libBOSSRemoteBinaryLoaderEngine.so";
inline std::string WD_PATH =
    "/data/david/BOSSWisentDeserialiserEngine/"
    "release/libBOSSWisentDeserialiserEngine.so";
inline std::string DE_PATH =
    "/data/david/BOSSDictionaryEncoderEngine/"
    "releaseBuild/libBOSSDictionaryEncoderEngine.so";
inline std::string VELOX_PATH =
    "/data/david/BOSSVeloxEngine/extendedActualReleaseBuild/"
    "libBOSSVeloxEngine.so";
inline std::string PR_PATH =
    "/data/david/BOSSParquetReaderEngine/releaseBuild/"
    "libBOSSParquetReaderEngine.so";
inline std::string CE_PATH =
    "/data/david/BOSSConditionalEvaluationEngine/releaseBuild/"
    "libBOSSConditionalEvaluationEngine.so";
inline std::string coordinator =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/libBOSSLazyLoadingCoordinatorEngine.so";

inline std::string OVERHEAD_CSV_SIZE_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_size_overhead.csv";
inline std::string OVERHEAD_CSV_TIME_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_time_overhead.csv";

inline std::string QUERY_OVERHEAD_CSV_SIZE_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/query_0_transfer_size_overhead.csv";
inline std::string QUERY_OVERHEAD_CSV_TIME_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/query_0_transfer_time_overhead.csv";

inline std::string MINISEED_OVERHEAD_CSV_SIZE_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/miniseed_transfer_size_overhead.csv";
inline std::string MINISEED_OVERHEAD_CSV_TIME_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/miniseed_transfer_time_overhead.csv";

inline std::string TPCH_OVERHEAD_CSV_SIZE_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/FAKE_PARQUET_LIGHT_COMPRESSION_RESULTS/tpch_transfer_size_overhead.csv";
inline std::string TPCH_OVERHEAD_CSV_TIME_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/FAKE_PARQUET_LIGHT_COMPRESSION_RESULTS/tpch_transfer_time_overhead.csv";

inline std::string IN_MEM_CONV_CSV_SIZE_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_size_overhead_mem_conv.csv";
inline std::string IN_MEM_CONV_CSV_TIME_PATH =
    "/data/david/BOSSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_time_overhead_mem_conv.csv";
} // namespace boss::benchmarks::LazyLoading::config::paths
