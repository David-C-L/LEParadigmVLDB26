#include "config.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include "utilities.hpp"

#pragma once

namespace boss::benchmarks::LazyLoading::MiniseedQueries {

using namespace std;
using namespace boss;
  using boss::utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using boss::benchmarks::LazyLoading::utilities::wrapEval;
using boss::benchmarks::LazyLoading::config::paths::RBL_PATH;

int64_t TIME_2019_08_02_02_00 = 1564711200000000000;
int64_t TIME_2019_08_02_02_02 = 1564711320000000000;
int64_t TIME_2019_08_02_02_30 = 1564713000000000000;
int64_t TIME_2019_08_02_03_00 = 1564714800000000000;
int64_t TIME_2019_08_02_14_00 = 1564754400000000000;
std::string DENORMALISED_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/denormalised_table.bin";
std::string FILES_TABLE_URL = "https://www.doc.ic.ac.uk/~dcl19/files_table.bin";
std::string CATALOG_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/catalog_table.bin";
std::string RANGED_FILES_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/rangedFilesTable.bin";
std::string RANGED_CATALOG_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/rangedCatalogTable.bin";
std::string RANGED_DATA_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/rangedDataTable.bin";

inline std::vector<std::function<boss::Expression()>> bossQueries{
    [] {
      auto firstGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("start_time"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject =
          "Project"_(std::move(firstEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "start_time"_, "start_time"_));
      auto firstOuterIndicesProject = "Project"_(
          "Select"_(std::move(firstIndicesProject),
                    "Where"_("And"_(
                        "Greater"_("start_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_03_00, "start_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto secondGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(firstOuterIndicesProject), "List"_("channel"_));
      auto secondEncodeIndices = "EncodeTable"_(std::move(secondGatherExpr));
      auto secondIndicesProject =
          "Project"_(std::move(secondEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "channel"_, "channel"_));
      auto secondOuterIndicesProject =
          "Project"_("Select"_(std::move(secondIndicesProject),
                               "Where"_("Equal"_("channel"_, 0))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto gatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(secondOuterIndicesProject), "List"_("network"_));
      auto encodeIndices = "EncodeTable"_(std::move(gatherExpr));
      auto indicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "network"_, "network"_));
      auto outerIndicesProject =
          "Project"_("Select"_(std::move(indicesProject),
                               "Where"_("Equal"_("network"_, 0))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));

      auto outerGather = "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                                   std::move(outerIndicesProject),
                                   "List"_("sample_value"_, "channel"_));
      auto encodeOuterGather = "EncodeTable"_(std::move(outerGather));
      auto groupBy =
          "Group"_("Project"_(std::move(encodeOuterGather),
                              "As"_("sample_value"_, "sample_value"_,
                                    "channel"_, "channel"_)),
                   "By"_("channel"_),
                   "As"_("sum_sample_value"_, "Sum"_("sample_value"_)));
      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));
      return std::move(decodeGroupBy);
    },
    [] {
      auto firstGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("sample_time"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject =
          "Project"_(std::move(firstEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "sample_time"_, "sample_time"_));
      auto firstOuterIndicesProject = "Project"_(
          "Select"_(std::move(firstIndicesProject),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_02_02, "sample_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto firstOuterGather = "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                                        std::move(firstOuterIndicesProject),
                                        "List"_("station"_, "channel"_));
      auto encodeIndices = "EncodeTable"_(std::move(firstOuterGather));
      auto innerIndicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "station"_, "station"_, "channel"_, "channel"_));
      auto indicesProject =
          "Project"_("Select"_(std::move(innerIndicesProject),
                               "Where"_("And"_("Equal"_("station"_, 0),
                                               "Equal"_("channel"_, 0)))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto outerGather =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, std::move(indicesProject),
                    "List"_("sample_value"_));
      auto groupBy =
          "Group"_("Project"_(std::move(outerGather),
                              "As"_("sample_value"_, "sample_value"_)),
                   "Sum"_("sample_value"_));
      return std::move(groupBy);
    },
    [] {
      auto firstGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("sample_time"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject =
          "Project"_(std::move(firstEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "sample_time"_, "sample_time"_));

      auto firstOuterIndicesProject = "Project"_(
          "Select"_(std::move(firstIndicesProject),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_02_02, "sample_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto indicesGatherExpr = "Gather"_(
          DENORMALISED_TABLE_URL, RBL_PATH, std::move(firstOuterIndicesProject),
          "List"_("station"_, "channel"_, "start_time"_));
      auto encodeIndices = "EncodeTable"_(std::move(indicesGatherExpr));
      auto innerIndicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "station"_, "station"_, "channel"_, "channel"_,
                           "start_time"_, "start_time"_));

      auto indicesProject = "Project"_(
          "Select"_(std::move(innerIndicesProject),
                    "Where"_("And"_(
                        "Equal"_("station"_, 0), "Equal"_("channel"_, 0),
                        "Greater"_("start_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_14_00, "start_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto outerGather =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, std::move(indicesProject),
                    "List"_("sample_value"_));
      auto groupBy =
          "Group"_("Project"_(std::move(outerGather),
                              "As"_("sample_value"_, "sample_value"_)),
                   "Sum"_("sample_value"_));
      return std::move(groupBy);
    },
    [] {
      auto firstGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("sample_time"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject = "Project"_(std::move(firstEncodeIndices),
                                            "As"_("__internal_indices_"_,
                                                  "__internal_indices_"_
                                                  "sample_time"_,
                                                  "sample_time"_));
      auto firstOuterIndicesProject = "Project"_(
          "Select"_(std::move(firstIndicesProject),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_02_02, "sample_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto indicesGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(firstOuterIndicesProject), "List"_("station"_));
      auto encodeIndices = "EncodeTable"_(std::move(indicesGatherExpr));
      auto innerIndicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "station"_, "station"_));
      auto indicesProject =
          "Project"_("Select"_(std::move(innerIndicesProject),
                               "Where"_("And"_("Equal"_("station"_, 0)))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto outerGather =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, std::move(indicesProject),
                    "List"_("sample_value"_, "channel"_));
      auto encodeOuterGather = "EncodeTable"_(std::move(outerGather));
      auto groupBy =
          "Group"_("Project"_(std::move(encodeOuterGather),
                              "As"_("sample_value"_, "sample_value"_,
                                    "channel"_, "channel"_)),
                   "By"_("channel"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_)));

      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));
      return std::move(decodeGroupBy);
    },
    [] {
      auto firstGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("sample_time"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject =
          "Project"_(std::move(firstEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "sample_time"_, "sample_time"_));
      auto firstOuterIndicesProject = "Project"_(
          "Select"_(std::move(firstIndicesProject),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_02_02, "sample_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto indicesGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(firstOuterIndicesProject), "List"_("station"_));
      auto encodeIndices = "EncodeTable"_(std::move(indicesGatherExpr));
      auto innerIndicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "station"_, "station"_));
      auto indicesProject =
          "Project"_("Select"_(std::move(innerIndicesProject),
                               "Where"_("And"_("Equal"_("station"_, 0)))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto outerGather =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, std::move(indicesProject),
                    "List"_("sample_value"_, "sample_time"_, "channel"_));
      return std::move(outerGather);
    },
    [] {
      auto firstGatherExpr = "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                                       "List"_("List"_()), "List"_("channel"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject =
          "Project"_(std::move(firstEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "channel"_, "channel"_));
      auto firstOuterIndicesProject =
          "Project"_("Select"_(std::move(firstIndicesProject),
                               "Where"_("Equal"_("channel"_, 0))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto indicesGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(firstOuterIndicesProject), "List"_("network"_));
      auto encodeIndices = "EncodeTable"_(std::move(indicesGatherExpr));
      auto innerIndicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "network"_, "network"_));
      auto indicesProject =
          "Project"_("Select"_(std::move(innerIndicesProject),
                               "Where"_("Equal"_("network"_, 0))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto outerGather =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, std::move(indicesProject),
                    "List"_("sample_value"_, "station"_));
      auto encodeOuterGather = "EncodeTable"_(std::move(outerGather));
      auto groupBy =
          "Group"_("Project"_(std::move(encodeOuterGather),
                              "As"_("sample_value"_, "sample_value"_,
                                    "station"_, "station"_)),
                   "By"_("station"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_),
                         "sum_sample_value"_, "Sum"_("sample_value"_)));

      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));
      return std::move(decodeGroupBy);
    },
    [] {
      auto firstGatherExpr = "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                                       "List"_("List"_()), "List"_("seq_no"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject =
          "Project"_(std::move(firstEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "seq_no"_, "seq_no"_));
      auto firstOuterIndicesProject =
          "Project"_("Select"_(std::move(firstIndicesProject),
                               "Where"_("Equal"_("seq_no"_, 0))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto secondGatherExpr = "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                                        std::move(firstOuterIndicesProject),
                                        "List"_("start_time"_));
      auto secondEncodeIndices = "EncodeTable"_(std::move(secondGatherExpr));
      auto secondIndicesProject =
          "Project"_(std::move(secondEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "start_time"_, "start_time"_));
      auto secondOuterIndicesProject = "Project"_(
          "Select"_(std::move(secondIndicesProject),
                    "Where"_("And"_(
                        "Greater"_("start_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_03_00, "start_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto indicesGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(secondOuterIndicesProject),
                    "List"_("network"_, "station"_, "location"_, "channel"_));
      auto encodeIndices = "EncodeTable"_(std::move(indicesGatherExpr));
      auto innerIndicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "network"_, "network"_, "station"_, "station"_,
                           "location"_, "location"_, "channel"_, "channel"_));
      auto indicesProject = "Project"_(
          "Select"_(std::move(innerIndicesProject),
                    "Where"_("And"_(
                        "Equal"_("network"_, 0), "Equal"_("station"_, 0),
                        "Equal"_("location"_, 0), "Equal"_("channel"_, 0)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto outerGather =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, std::move(indicesProject),
                    "List"_("sample_value"_, "sample_time"_));
      return std::move(outerGather);
    },
    [] {
      auto firstGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("start_time"_));
      auto firstEncodeIndices = "EncodeTable"_(std::move(firstGatherExpr));
      auto firstIndicesProject =
          "Project"_(std::move(firstEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "start_time"_, "start_time"_));
      auto firstOuterIndicesProject = "Project"_(
          "Select"_(std::move(firstIndicesProject),
                    "Where"_("And"_(
                        "Greater"_("start_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_03_00, "start_time"_)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto secondGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(firstOuterIndicesProject), "List"_("seq_no"_));
      auto secondEncodeIndices = "EncodeTable"_(std::move(secondGatherExpr));
      auto secondIndicesProject =
          "Project"_(std::move(secondEncodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "seq_no"_, "seq_no"_));
      auto secondOuterIndicesProject =
          "Project"_("Select"_(std::move(secondIndicesProject),
                               "Where"_("Equal"_("seq_no"_, 0))),
                     "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto indicesGatherExpr =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH,
                    std::move(secondOuterIndicesProject),
                    "List"_("network"_, "station"_, "location"_, "channel"_));
      auto encodeIndices = "EncodeTable"_(std::move(indicesGatherExpr));
      auto innerIndicesProject =
          "Project"_(std::move(encodeIndices),
                     "As"_("__internal_indices_"_, "__internal_indices_"_,
                           "network"_, "network"_, "station"_, "station"_,
                           "location"_, "location"_, "channel"_, "channel"_));
      auto indicesProject = "Project"_(
          "Select"_(std::move(innerIndicesProject),
                    "Where"_("And"_(
                        "Equal"_("network"_, 0), "Equal"_("station"_, 0),
                        "Equal"_("location"_, 1), "Equal"_("channel"_, 1)))),
          "As"_("__internal_indices_"_, "__internal_indices_"_));
      auto outerGather =
          "Gather"_(DENORMALISED_TABLE_URL, RBL_PATH, std::move(indicesProject),
                    "List"_("sample_value"_, "sample_time"_));
      return std::move(outerGather);
    }};

inline std::vector<std::function<boss::Expression()>> bossRangesQueries{
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
    },
    [] {
      auto filesGather =
          "Gather"_(RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("channel"_, "station"_, "f_start"_, "f_end"_));

      auto encodeFiles = "EncodeTable"_(std::move(filesGather));

      auto filesSelect = "Select"_(
          std::move(encodeFiles),
          "Where"_("And"_("Equal"_("station"_, 0), "Equal"_("channel"_, 0))));

      auto dataRangesGather = "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(filesSelect),
          std::move("List"_("sample_value"_, "sample_time"_)));

      auto dataProject = "Project"_(
          "Select"_(std::move(dataRangesGather),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_03_00, "sample_time"_)))),
          "As"_("sample_value"_, "sample_value"_));
      auto groupBy = "Group"_(std::move(dataProject), "Sum"_("sample_value"_));

      return std::move(groupBy);
    },
    [] {
      auto filesGather = "Gather"_(
          RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
          "List"_("channel"_, "station"_, "f_start"_, "f_end"_, "f_file_key"_));
      auto catalogGather =
          "Gather"_(RANGED_CATALOG_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("start_time"_, "c_start"_, "c_end"_, "c_file_key"_,
                            "c_seq_no"_));

      auto encodeFiles = "EncodeTable"_(std::move(filesGather));

      auto filesSelect = "Select"_(
          std::move(encodeFiles),
          "Where"_("And"_("Equal"_("station"_, 0), "Equal"_("channel"_, 0))));
      auto catalogSelect = "Select"_(
          std::move(catalogGather),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_14_00, "start_time"_))));
      auto filesCatalogJoin =
          "Project"_("Join"_(std::move(catalogSelect), std::move(filesSelect),
                             "Where"_("Equal"_("c_file_key"_, "f_file_key"_))),
                     "As"_("c_file_key"_, "c_file_key"_, "c_start"_, "c_start"_,
                           "c_end"_, "c_end"_, "c_seq_no"_, "c_seq_no"_));

      auto saveJoin =
          "SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_);

      auto dataRangesGather = "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("c_start"_),
          std::move("c_end"_), std::move(saveJoin),
          std::move("List"_("d_seq_no"_, "d_file_key"_, "sample_value"_,
                            "sample_time"_)));

      auto dataSelect = "Select"_(
          std::move(dataRangesGather),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getJoin = "GetTable"_("FilesCatalogTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(dataSelect), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_key"_, "d_seq_no"_),
                                    "List"_("c_file_key"_, "c_seq_no"_))));

      auto dataFilesCatalogProject =
          "Project"_(std::move(dataFilesCatalogJoin),
                     "As"_("sample_value"_, "sample_value"_));

      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "Sum"_("sample_value"_));

      return std::move(groupBy);
    },
    [] {
      auto filesGather = "Gather"_(
          RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
          "List"_("channel"_, "station"_, "f_start"_, "f_end"_, "f_file_key"_));

      auto encodeFiles = "EncodeTable"_(std::move(filesGather));

      auto filesSelect = "Project"_(
          "Select"_(std::move(encodeFiles), "Where"_("Equal"_("station"_, 0))),
          "As"_("channel"_, "channel"_, "f_start"_, "f_start"_, "f_end"_,
                "f_end"_, "f_file_key"_, "f_file_key"_));

      auto saveTable = "SaveTable"_(std::move(filesSelect), "FilesTable"_);

      auto dataRangesGather = "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(saveTable),
          std::move("List"_("d_file_key"_, "sample_value"_, "sample_time"_)));

      auto dataSelect = "Select"_(
          std::move(dataRangesGather),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesJoin =
          "Join"_(std::move(dataSelect), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_key"_, "f_file_key"_)));

      auto dataFilesProject = "Project"_(
          std::move(dataFilesJoin),
          "As"_("sample_value"_, "sample_value"_, "channel"_, "channel"_));

      auto groupBy =
          "Group"_(std::move(dataFilesProject), "By"_("channel"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_)));

      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    },
    [] {
      auto filesGather = "Gather"_(
          RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
          "List"_("channel"_, "station"_, "f_start"_, "f_end"_, "f_file_key"_));

      auto encodeFiles = "EncodeTable"_(std::move(filesGather));

      auto filesSelect = "Project"_(
          "Select"_(std::move(encodeFiles), "Where"_("Equal"_("station"_, 0))),
          "As"_("channel"_, "channel"_, "f_start"_, "f_start"_, "f_end"_,
                "f_end"_, "f_file_key"_, "f_file_key"_));

      auto saveTable = "SaveTable"_(std::move(filesSelect), "FilesTable"_);

      auto dataRangesGather = "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(saveTable),
          std::move("List"_("d_file_key"_, "sample_value"_, "sample_time"_)));

      auto dataSelect = "Select"_(
          std::move(dataRangesGather),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesJoin =
          "Join"_(std::move(dataSelect), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_key"_, "f_file_key"_)));

      auto dataFilesProject =
          "Project"_(std::move(dataFilesJoin),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_, "channel"_, "channel"_));

      return std::move(dataFilesProject);
    },
    [] {
      auto filesGather =
          "Gather"_(RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("network"_, "channel"_, "station"_, "f_start"_,
                            "f_end"_, "f_file_key"_));

      auto encodeFiles = "EncodeTable"_(std::move(filesGather));

      auto filesSelect =
          "Project"_("Select"_(std::move(encodeFiles),
                               "Where"_("And"_("Equal"_("network"_, 0),
                                               "Equal"_("channel"_, 0)))),
                     "As"_("station"_, "station"_, "f_start"_, "f_start"_,
                           "f_end"_, "f_end"_, "f_file_key"_, "f_file_key"_));

      auto saveTable = "SaveTable"_(std::move(filesSelect), "FilesTable"_);

      auto dataRangesGather = "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(saveTable),
          std::move("List"_("d_file_key"_, "sample_value"_)));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesJoin =
          "Join"_(std::move(dataRangesGather), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_key"_, "f_file_key"_)));

      auto dataFilesProject = "Project"_(
          std::move(dataFilesJoin),
          "As"_("sample_value"_, "sample_value"_, "station"_, "station"_));

      auto groupBy =
          "Group"_(std::move(dataFilesProject), "By"_("station"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_),
                         "sum_sample_value"_, "Sum"_("sample_value"_)));

      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    },
    [] {
      std::vector<int64_t> starts = {0};
      std::vector<int64_t> ends = {720000};
      auto rangeTable = "Table"_(
          "start"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(starts)))))),
          "end"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(ends)))))));
      auto dataRangesGather = "GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("start"_),
          std::move("end"_), std::move(rangeTable),
          std::move("List"_("d_seq_no"_, "sample_time"_, "sample_value"_)));

      auto dataSelect =
          "Project"_("Select"_(std::move(dataRangesGather),
                               "Where"_("Equal"_("d_seq_no"_, 0))),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_));

      return std::move(dataSelect);
    },
    [] {
      std::vector<int64_t> starts = {63360000};
      std::vector<int64_t> ends = {64080000};
      std::string falseAddress = RANGED_DATA_TABLE_URL + "_incorrect_endpoint.bin";
      auto rangeTable = "Table"_(
          "start"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(starts)))))),
          "end"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(ends)))))));
      auto dataRangesGather = "GatherRanges"_(
          falseAddress, RBL_PATH, std::move("start"_),
          std::move("end"_), std::move(rangeTable),
          std::move("List"_("d_seq_no"_, "sample_time"_, "sample_value"_)));

      auto dataSelect =
          "Project"_("Select"_(std::move(dataRangesGather),
                               "Where"_("Equal"_("d_seq_no"_, 1))),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_));

      return std::move(dataSelect);
    }};

  inline std::vector<std::function<boss::Expression()>> bossCycleRangesQueries{
    [] {
      auto filesGather =
	wrapEval("Gather"_(RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("network"_, "channel"_, "station"_, "f_start"_,
                            "f_end"_, "f_file_key"_)), 0);
      auto catalogGather =
	wrapEval("Gather"_(RANGED_CATALOG_TABLE_URL, RBL_PATH, "List"_("List"_()),
				     "List"_("start_time"_, "c_start"_, "c_end"_, "c_file_key"_,
					     "c_seq_no"_)), 1);
      
      auto encodeFiles = wrapEval("EncodeTable"_(std::move(filesGather)), 0);

      auto filesSelect = wrapEval("Select"_(
          std::move(encodeFiles),
          "Where"_("And"_("Equal"_("network"_, 0), "Equal"_("channel"_, 0)))), 0);
      auto catalogSelect = wrapEval("Select"_(
          std::move(catalogGather),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "start_time"_)))),1);
      auto filesCatalogJoin = wrapEval("Project"_(
						  wrapEval("Join"_(std::move(catalogSelect), std::move(filesSelect),
								   "Where"_("Equal"_("c_file_key"_, "f_file_key"_))), 1),
          "As"_("c_file_key"_, "c_file_key"_, "c_start"_, "c_start"_, "c_end"_,
                "c_end"_, "c_seq_no"_, "c_seq_no"_, "station"_, "station"_)), 1);

      auto saveJoin =
	wrapEval("SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_), 2);

      auto rangesGather = wrapEval("GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("c_start"_),
          std::move("c_end"_), std::move(saveJoin),
          std::move("List"_("d_seq_no"_, "d_file_key"_, "sample_value"_))), 3);

      auto getJoin = wrapEval("GetTable"_("FilesCatalogTable"_), 4);

      auto dataFilesCatalogJoin =
	wrapEval("Join"_(std::move(rangesGather), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_key"_, "d_seq_no"_),
                                    "List"_("c_file_key"_, "c_seq_no"_)))), 4);

      auto dataFilesCatalogProject = wrapEval("Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("station"_, "station"_, "sample_value"_, "sample_value"_)), 4);

      auto groupBy =
	wrapEval("Group"_(std::move(dataFilesCatalogProject), "By"_("station"_),
			  "As"_("sum_sample_value"_, "Sum"_("sample_value"_))), 4);
      auto decodeGroupBy = wrapEval("DecodeTable"_(std::move(groupBy)), 5);

      return std::move(decodeGroupBy);
    },
    [] {
      auto filesGather =
	wrapEval("Gather"_(RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
			   "List"_("channel"_, "station"_, "f_start"_, "f_end"_)), 0);

      auto encodeFiles = wrapEval("EncodeTable"_(std::move(filesGather)), 0);

      auto filesSelect = wrapEval("Select"_(
          std::move(encodeFiles),
          "Where"_("And"_("Equal"_("station"_, 0), "Equal"_("channel"_, 0)))), 0);

      auto dataRangesGather = wrapEval("GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(filesSelect),
          std::move("List"_("sample_value"_, "sample_time"_))), 1);

      auto dataProject = wrapEval("Project"_(
          wrapEval("Select"_(std::move(dataRangesGather),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_03_00, "sample_time"_)))), 1),
          "As"_("sample_value"_, "sample_value"_)), 1);
      auto groupBy = wrapEval("Group"_(std::move(dataProject), "Sum"_("sample_value"_)), 1);

      return std::move(groupBy);
    },
    [] {
      auto filesGather = wrapEval("Gather"_(
          RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
          "List"_("channel"_, "station"_, "f_start"_, "f_end"_, "f_file_key"_)), 0);
      auto catalogGather =
	wrapEval("Gather"_(RANGED_CATALOG_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("start_time"_, "c_start"_, "c_end"_, "c_file_key"_,
                            "c_seq_no"_)), 1);

      auto encodeFiles = wrapEval("EncodeTable"_(std::move(filesGather)), 0);

      auto filesSelect = wrapEval("Select"_(
          std::move(encodeFiles),
          "Where"_("And"_("Equal"_("station"_, 0), "Equal"_("channel"_, 0)))), 0);
      auto catalogSelect = wrapEval("Select"_(
          std::move(catalogGather),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_14_00, "start_time"_)))), 1);
      auto filesCatalogJoin =
          wrapEval("Project"_(wrapEval("Join"_(std::move(catalogSelect), std::move(filesSelect),
					       "Where"_("Equal"_("c_file_key"_, "f_file_key"_))), 1),
                     "As"_("c_file_key"_, "c_file_key"_, "c_start"_, "c_start"_,
                           "c_end"_, "c_end"_, "c_seq_no"_, "c_seq_no"_)), 1);

      auto saveJoin =
	wrapEval("SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_), 2);

      auto dataRangesGather = wrapEval("GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("c_start"_),
          std::move("c_end"_), std::move(saveJoin),
          std::move("List"_("d_seq_no"_, "d_file_key"_, "sample_value"_,
                            "sample_time"_))), 3);

      auto dataSelect = wrapEval("Select"_(
          std::move(dataRangesGather),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_)))), 3);

      auto getJoin = wrapEval("GetTable"_("FilesCatalogTable"_), 4);

      auto dataFilesCatalogJoin =
          wrapEval("Join"_(std::move(dataSelect), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_key"_, "d_seq_no"_),
                                    "List"_("c_file_key"_, "c_seq_no"_)))), 4);

      auto dataFilesCatalogProject =
          wrapEval("Project"_(std::move(dataFilesCatalogJoin),
			      "As"_("sample_value"_, "sample_value"_)), 4);

      auto groupBy =
	wrapEval("Group"_(std::move(dataFilesCatalogProject), "Sum"_("sample_value"_)), 4);

      return std::move(groupBy);
    },
    [] {
      auto filesGather = wrapEval("Gather"_(
          RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
          "List"_("channel"_, "station"_, "f_start"_, "f_end"_, "f_file_key"_)), 0);

      auto encodeFiles = wrapEval("EncodeTable"_(std::move(filesGather)), 0);

      auto filesSelect = wrapEval("Project"_(
					     wrapEval("Select"_(std::move(encodeFiles), "Where"_("Equal"_("station"_, 0))), 0),
          "As"_("channel"_, "channel"_, "f_start"_, "f_start"_, "f_end"_,
                "f_end"_, "f_file_key"_, "f_file_key"_)), 0);

      auto saveTable = wrapEval("SaveTable"_(std::move(filesSelect), "FilesTable"_), 1);

      auto dataRangesGather = wrapEval("GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(saveTable),
          std::move("List"_("d_file_key"_, "sample_value"_, "sample_time"_))), 2);

      auto dataSelect = wrapEval("Select"_(
          std::move(dataRangesGather),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_)))), 2);

      auto getFilesSelect = wrapEval("GetTable"_("FilesTable"_), 3);

      auto dataFilesJoin =
          wrapEval("Join"_(std::move(dataSelect), std::move(getFilesSelect),
			   "Where"_("Equal"_("d_file_key"_, "f_file_key"_))), 3);

      auto dataFilesProject = wrapEval("Project"_(
          std::move(dataFilesJoin),
          "As"_("sample_value"_, "sample_value"_, "channel"_, "channel"_)), 3);

      auto groupBy =
          wrapEval("Group"_(std::move(dataFilesProject), "By"_("channel"_),
			    "As"_("avg_sample_value"_, "Avg"_("sample_value"_))), 3);

      auto decodeGroupBy = wrapEval("DecodeTable"_(std::move(groupBy)), 4);

      return std::move(decodeGroupBy);
    },
    [] {
      auto filesGather = wrapEval("Gather"_(
          RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
          "List"_("channel"_, "station"_, "f_start"_, "f_end"_, "f_file_key"_)), 0);

      auto encodeFiles = wrapEval("EncodeTable"_(std::move(filesGather)), 0);

      auto filesSelect = wrapEval("Project"_(
					     wrapEval("Select"_(std::move(encodeFiles), "Where"_("Equal"_("station"_, 0))), 0),
          "As"_("channel"_, "channel"_, "f_start"_, "f_start"_, "f_end"_,
                "f_end"_, "f_file_key"_, "f_file_key"_)), 0);

      auto saveTable = wrapEval("SaveTable"_(std::move(filesSelect), "FilesTable"_), 1);

      auto dataRangesGather = wrapEval("GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(saveTable),
          std::move("List"_("d_file_key"_, "sample_value"_, "sample_time"_))), 2);

      auto dataSelect = wrapEval("Select"_(
          std::move(dataRangesGather),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_)))), 2);

      auto getFilesSelect = wrapEval("GetTable"_("FilesTable"_), 3);

      auto dataFilesJoin =
          wrapEval("Join"_(std::move(dataSelect), std::move(getFilesSelect),
			   "Where"_("Equal"_("d_file_key"_, "f_file_key"_))), 4);

      auto dataFilesProject =
          wrapEval("Project"_(std::move(dataFilesJoin),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_, "channel"_, "channel"_)), 4);

      return std::move(dataFilesProject);
    },
    [] {
      auto filesGather =
          wrapEval("Gather"_(RANGED_FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
                    "List"_("network"_, "channel"_, "station"_, "f_start"_,
                            "f_end"_, "f_file_key"_)), 0);

      auto encodeFiles = wrapEval("EncodeTable"_(std::move(filesGather)), 0);

      auto filesSelect =
          wrapEval("Project"_(wrapEval("Select"_(std::move(encodeFiles),
                               "Where"_("And"_("Equal"_("network"_, 0),
                                               "Equal"_("channel"_, 0)))), 0),
                     "As"_("station"_, "station"_, "f_start"_, "f_start"_,
                           "f_end"_, "f_end"_, "f_file_key"_, "f_file_key"_)), 0);

      auto saveTable = wrapEval("SaveTable"_(std::move(filesSelect), "FilesTable"_), 1);

      auto dataRangesGather = wrapEval("GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("f_start"_),
          std::move("f_end"_), std::move(saveTable),
          std::move("List"_("d_file_key"_, "sample_value"_))), 2);

      auto getFilesSelect = wrapEval("GetTable"_("FilesTable"_), 3);

      auto dataFilesJoin =
          wrapEval("Join"_(std::move(dataRangesGather), std::move(getFilesSelect),
			   "Where"_("Equal"_("d_file_key"_, "f_file_key"_))), 4);

      auto dataFilesProject = wrapEval("Project"_(
          std::move(dataFilesJoin),
          "As"_("sample_value"_, "sample_value"_, "station"_, "station"_)), 4);

      auto groupBy =
          wrapEval("Group"_(std::move(dataFilesProject), "By"_("station"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_),
                         "sum_sample_value"_, "Sum"_("sample_value"_))), 4);

      auto decodeGroupBy = wrapEval("DecodeTable"_(std::move(groupBy)), 5);

      return std::move(decodeGroupBy);
    },
    [] {
      std::vector<int64_t> starts = {0};
      std::vector<int64_t> ends = {720000};
      auto rangeTable = "Table"_(
          "start"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(starts)))))),
          "end"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(ends)))))));
      auto dataRangesGather = wrapEval("GatherRanges"_(
          RANGED_DATA_TABLE_URL, RBL_PATH, std::move("start"_),
          std::move("end"_), std::move(rangeTable),
          std::move("List"_("d_seq_no"_, "sample_time"_, "sample_value"_))), 0);

      auto dataSelect =
          wrapEval("Project"_(wrapEval("Select"_(std::move(dataRangesGather),
						 "Where"_("Equal"_("d_seq_no"_, 0))), 0),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_)), 0);

      return std::move(dataSelect);
    },
    [] {
      std::vector<int64_t> starts = {63360000};
      std::vector<int64_t> ends = {64080000};
      std::string falseAddress = RANGED_DATA_TABLE_URL + "_incorrect_endpoint.bin";
      auto rangeTable = "Table"_(
          "start"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(starts)))))),
          "end"_("List"_(
              std::move(boss::Span<int64_t const>(std::move(vector(ends)))))));
      auto dataRangesGather = wrapEval("GatherRanges"_(
          falseAddress, RBL_PATH, std::move("start"_),
          std::move("end"_), std::move(rangeTable),
          std::move("List"_("d_seq_no"_, "sample_time"_, "sample_value"_))), 0);

      auto dataSelect =
          wrapEval("Project"_(wrapEval("Select"_(std::move(dataRangesGather),
						 "Where"_("Equal"_("d_seq_no"_, 1))), 0),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_)), 0);

      return std::move(dataSelect);
    }};

inline std::vector<std::function<boss::Expression()>> dataVaultsQueries{
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
    },
    [] {
      std::vector<std::string> initialURLFiles = {FILES_TABLE_URL};
      auto eagerLoadFilesExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLFiles)))))));

      auto encodeFilesExpr = "EncodeTable"_(std::move(eagerLoadFilesExpr));
      auto innerFilesProject =
          "Project"_(std::move(encodeFilesExpr),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_, "station"_, "station"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("And"_("Equal"_("station"_, 0),
                                               "Equal"_("channel"_, 0)))),
                     "As"_("f_file_location"_, "f_file_location"));

      auto fileLocationDecoded = "DecodeTable"_(std::move(innerFilesSelect));
      auto lazyLoadDataExpr =
          "ParseTables"_(RBL_PATH, std::move("f_file_location"_),
                         std::move(fileLocationDecoded));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto dataProject = "Project"_(
          "Select"_(std::move(encodeLazyLoadData),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_03_00, "sample_time")))),
          "As"_("sample_value"_, "sample_value"_));

      auto groupBy = "Group"_(std::move(dataProject), "Sum"_("sample_value"_));

      return std::move(groupBy);
    },
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
          "Where"_("And"_("Equal"_("station"_, 0), "Equal"_("channel"_, 0))));
      auto innerCatalogSelect = "Select"_(
          std::move(encodeCatalogExpr),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_14_00, "start_time"_))));
      auto filesCatalogJoin = "Project"_(
          "Join"_(std::move(innerFilesSelect), std::move(innerCatalogSelect),
                  "Where"_("Equal"_("f_file_location"_, "c_file_location"_))),
          "As"_("c_file_location"_, "c_file_location"_, "c_seq_no"_,
                "c_seq_no"_));

      auto saveJoin =
          "SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveJoin));
      auto lazyLoadDataExpr =
          "ParseTables"_(RBL_PATH, std::move("c_file_location"_),
                         std::move(fileLocationDecoded));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto innerDataSelect = "Select"_(
          std::move(encodeLazyLoadData),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getJoin = "GetTable"_("FilesCatalogTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(innerDataSelect), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_location"_, "d_seq_no"_),
                                    "List"_("c_file_location"_, "c_seq_no"_))));

      auto dataFilesCatalogProject =
          "Project"_(std::move(dataFilesCatalogJoin),
                     "As"_("sample_value"_, "sample_value"_));
      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "Sum"_("sample_value"_));

      return std::move(groupBy);
    },
    [] {
      std::vector<std::string> initialURLFiles = {FILES_TABLE_URL};
      auto eagerLoadFilesExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLFiles)))))));
      auto encodeFilesExpr = "EncodeTable"_(std::move(eagerLoadFilesExpr));
      auto innerFilesProject =
          "Project"_(std::move(encodeFilesExpr),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_, "station"_, "station"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("Equal"_("station"_, 0))),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_));

      auto saveTable = "SaveTable"_(std::move(innerFilesSelect), "FilesTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveTable));
      auto lazyLoadDataExpr =
          "ParseTables"_(RBL_PATH, std::move("f_file_location"_),
                         std::move(fileLocationDecoded));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto innerDataSelect = "Select"_(
          std::move(encodeLazyLoadData),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(innerDataSelect), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_location"_, "f_file_location"_)));

      auto dataFilesCatalogProject = "Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("sample_value"_, "sample_value"_, "channel"_, "channel"_));

      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "By"_("channel"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_)));
      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    },
    [] {
      std::vector<std::string> initialURLFiles = {FILES_TABLE_URL};
      auto eagerLoadFilesExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLFiles)))))));
      auto encodeFilesExpr = "EncodeTable"_(std::move(eagerLoadFilesExpr));
      auto innerFilesProject =
          "Project"_(std::move(encodeFilesExpr),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_, "station"_, "station"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("Equal"_("station"_, 0))),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_));

      auto saveTable = "SaveTable"_(std::move(innerFilesSelect), "FilesTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveTable));
      auto lazyLoadDataExpr =
          "ParseTables"_(RBL_PATH, std::move("f_file_location"_),
                         std::move(fileLocationDecoded));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto innerDataSelect = "Select"_(
          std::move(encodeLazyLoadData),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(innerDataSelect), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_location"_, "f_file_location"_)));

      auto dataFilesCatalogProject =
          "Project"_(std::move(dataFilesCatalogJoin),
                     "As"_("sample_value"_, "sample_value"_, "sample_time"_,
                           "sample_time"_, "channel"_, "channel"_));
      auto decodeProject = "DecodeTable"_(std::move(dataFilesCatalogProject));

      return std::move(decodeProject);
    },
    [] {
      std::vector<std::string> initialURLFiles = {FILES_TABLE_URL};
      auto eagerLoadFilesExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLFiles)))))));
      auto encodeFilesExpr = "EncodeTable"_(std::move(eagerLoadFilesExpr));
      auto innerFilesProject = "Project"_(
          std::move(encodeFilesExpr),
          "As"_("f_file_location"_, "f_file_location"_, "channel"_, "channel"_,
                "station"_, "station"_, "network"_, "network"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("And"_("Equal"_("network"_, 0),
                                               "Equal"_("channel"_, 0)))),
                     "As"_("f_file_location"_, "f_file_location"_, "station"_,
                           "station"_));

      auto saveTable = "SaveTable"_(std::move(innerFilesSelect), "FilesTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveTable));
      auto lazyLoadDataExpr =
          "ParseTables"_(RBL_PATH, std::move("f_file_location"_),
                         std::move(fileLocationDecoded));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(encodeLazyLoadData), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_location"_, "f_file_location"_)));

      auto dataFilesCatalogProject = "Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("sample_value"_, "sample_value"_, "station"_, "station"_));
      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "By"_("station"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_),
                         "sum_sample_value"_, "Sum"_("sample_value"_)));
      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    },
    [] {
      std::vector<std::string> initialURLData = {
          "https://www.doc.ic.ac.uk/~dcl19/2019-08-02-0200-00M.PNR02_HHE.bin"};
      auto eagerLoadDataExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLData)))))));
      auto encodeDataExpr = "EncodeTable"_(std::move(eagerLoadDataExpr));
      auto innerDataProject =
          "Project"_(std::move(encodeDataExpr),
                     "As"_("d_seq_no"_, "d_seq_no"_, "sample_time"_,
                           "sample_time"_, "sample_value"_, "sample_value"_));
      auto innerDataSelect =
          "Project"_("Select"_(std::move(innerDataProject),
                               "Where"_("Equal"_("d_seq_no"_, 0))),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_));

      return std::move(innerDataSelect);
    },
    [] {
      std::vector<std::string> initialURLData = {
          "https://www.doc.ic.ac.uk/~dcl19/2019-08-02-0200-00M.PNR04_HHN.bin_incorrectEndpoint.bin"};
      auto eagerLoadDataExpr = "ParseTables"_(
          RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                        std::move(vector(initialURLData)))))));
      auto encodeDataExpr = "EncodeTable"_(std::move(eagerLoadDataExpr));
      auto innerDataProject =
          "Project"_(std::move(encodeDataExpr),
                     "As"_("d_seq_no"_, "d_seq_no"_, "sample_time"_,
                           "sample_time"_, "sample_value"_, "sample_value"_));
      auto innerDataSelect =
          "Project"_("Select"_(std::move(innerDataProject),
                               "Where"_("Equal"_("d_seq_no"_, 1))),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_));

      return std::move(innerDataSelect);
    }};

inline std::vector<std::function<boss::Expression()>> dataVaultsColumnGranularityQueries{
    [] {
      
      auto filesGather = "Gather"_(FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
				   "List"_("channel"_, "network"_, "station"_, "f_file_location"_));
      auto catalogGather = "Gather"_(CATALOG_TABLE_URL, RBL_PATH, "List"_("List"_()),
				     "List"_("start_time"_, "c_file_location"_, "c_seq_no"_));
      
      auto encodeFilesExpr = "EncodeTable"_(std::move(filesGather));
      auto encodeCatalogExpr = "EncodeTable"_(std::move(catalogGather));

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
          "GatherTables"_(RBL_PATH, std::move("c_file_location"_),
			  std::move(fileLocationDecoded), "List"_("List"_()),
			  "List"_("d_file_location"_, "d_seq_no"_, "sample_value"_));
      
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
    },
    [] {

      auto filesGather = "Gather"_(FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
				   "List"_("channel"_, "station"_, "f_file_location"_));
      auto encodeFilesExpr = "EncodeTable"_(std::move(filesGather));
      auto innerFilesProject =
          "Project"_(std::move(encodeFilesExpr),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_, "station"_, "station"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("And"_("Equal"_("station"_, 0),
                                               "Equal"_("channel"_, 0)))),
                     "As"_("f_file_location"_, "f_file_location"));

      auto fileLocationDecoded = "DecodeTable"_(std::move(innerFilesSelect));
      auto lazyLoadDataExpr =
          "GatherTables"_(RBL_PATH, std::move("f_file_location"_),
			  std::move(fileLocationDecoded), "List"_("List"_()),
			  "List"_("sample_time"_, "sample_value"_));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto dataProject = "Project"_(
          "Select"_(std::move(encodeLazyLoadData),
                    "Where"_("And"_(
                        "Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                        "Greater"_(TIME_2019_08_02_03_00, "sample_time")))),
          "As"_("sample_value"_, "sample_value"_));

      auto groupBy = "Group"_(std::move(dataProject), "Sum"_("sample_value"_));

      return std::move(groupBy);
    },
    [] {
      
      auto filesGather = "Gather"_(FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
				   "List"_("channel"_, "station"_, "f_file_location"_));
      auto catalogGather = "Gather"_(CATALOG_TABLE_URL, RBL_PATH, "List"_("List"_()),
				     "List"_("start_time"_, "c_file_location"_, "c_seq_no"_));

      auto encodeFilesExpr = "EncodeTable"_(std::move(filesGather));
      auto encodeCatalogExpr = "EncodeTable"_(std::move(catalogGather));

      auto innerFilesSelect = "Select"_(
          std::move(encodeFilesExpr),
          "Where"_("And"_("Equal"_("station"_, 0), "Equal"_("channel"_, 0))));
      auto innerCatalogSelect = "Select"_(
          std::move(encodeCatalogExpr),
          "Where"_("And"_("Greater"_("start_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_14_00, "start_time"_))));
      auto filesCatalogJoin = "Project"_(
          "Join"_(std::move(innerFilesSelect), std::move(innerCatalogSelect),
                  "Where"_("Equal"_("f_file_location"_, "c_file_location"_))),
          "As"_("c_file_location"_, "c_file_location"_, "c_seq_no"_,
                "c_seq_no"_));

      auto saveJoin =
          "SaveTable"_(std::move(filesCatalogJoin), "FilesCatalogTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveJoin));
      auto lazyLoadDataExpr =
          "ParseTables"_(RBL_PATH, std::move("c_file_location"_),
                         std::move(fileLocationDecoded));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto innerDataSelect = "Select"_(
          std::move(encodeLazyLoadData),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getJoin = "GetTable"_("FilesCatalogTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(innerDataSelect), std::move(getJoin),
                  "Where"_("Equal"_("List"_("d_file_location"_, "d_seq_no"_),
                                    "List"_("c_file_location"_, "c_seq_no"_))));

      auto dataFilesCatalogProject =
          "Project"_(std::move(dataFilesCatalogJoin),
                     "As"_("sample_value"_, "sample_value"_));
      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "Sum"_("sample_value"_));

      return std::move(groupBy);
    },
    [] {
      auto filesGather = "Gather"_(FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
				   "List"_("channel"_, "station"_, "f_file_location"_));
      auto encodeFilesExpr = "EncodeTable"_(std::move(filesGather));
      auto innerFilesProject =
          "Project"_(std::move(encodeFilesExpr),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_, "station"_, "station"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("Equal"_("station"_, 0))),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_));

      auto saveTable = "SaveTable"_(std::move(innerFilesSelect), "FilesTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveTable));
      auto lazyLoadDataExpr =
          "GatherTables"_(RBL_PATH, std::move("f_file_location"_),
			  std::move(fileLocationDecoded), "List"_("List"_()),
			  "List"_("sample_time"_, "sample_value"_, "d_file_location"_));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto innerDataSelect = "Select"_(
          std::move(encodeLazyLoadData),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(innerDataSelect), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_location"_, "f_file_location"_)));

      auto dataFilesCatalogProject = "Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("sample_value"_, "sample_value"_, "channel"_, "channel"_));

      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "By"_("channel"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_)));
      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    },
    [] {
      auto filesGather = "Gather"_(FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
				   "List"_("channel"_, "station"_, "f_file_location"_));
      auto encodeFilesExpr = "EncodeTable"_(std::move(filesGather));
      auto innerFilesProject =
          "Project"_(std::move(encodeFilesExpr),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_, "station"_, "station"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("Equal"_("station"_, 0))),
                     "As"_("f_file_location"_, "f_file_location"_, "channel"_,
                           "channel"_));

      auto saveTable = "SaveTable"_(std::move(innerFilesSelect), "FilesTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveTable));
      auto lazyLoadDataExpr =
          "GatherTables"_(RBL_PATH, std::move("f_file_location"_),
			  std::move(fileLocationDecoded), "List"_("List"_()),
			  "List"_("sample_time"_, "sample_value"_, "d_file_location"_));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto innerDataSelect = "Select"_(
          std::move(encodeLazyLoadData),
          "Where"_("And"_("Greater"_("sample_time"_, TIME_2019_08_02_02_00),
                          "Greater"_(TIME_2019_08_02_03_00, "sample_time"_))));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(innerDataSelect), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_location"_, "f_file_location"_)));

      auto dataFilesCatalogProject =
          "Project"_(std::move(dataFilesCatalogJoin),
                     "As"_("sample_value"_, "sample_value"_, "sample_time"_,
                           "sample_time"_, "channel"_, "channel"_));
      auto decodeProject = "DecodeTable"_(std::move(dataFilesCatalogProject));

      return std::move(decodeProject);
    },
    [] {
      auto filesGather = "Gather"_(FILES_TABLE_URL, RBL_PATH, "List"_("List"_()),
				   "List"_("channel"_, "station"_, "network"_, "f_file_location"_));
      auto encodeFilesExpr = "EncodeTable"_(std::move(filesGather));
      auto innerFilesProject = "Project"_(
          std::move(encodeFilesExpr),
          "As"_("f_file_location"_, "f_file_location"_, "channel"_, "channel"_,
                "station"_, "station"_, "network"_, "network"_));
      auto innerFilesSelect =
          "Project"_("Select"_(std::move(innerFilesProject),
                               "Where"_("And"_("Equal"_("network"_, 0),
                                               "Equal"_("channel"_, 0)))),
                     "As"_("f_file_location"_, "f_file_location"_, "station"_,
                           "station"_));

      auto saveTable = "SaveTable"_(std::move(innerFilesSelect), "FilesTable"_);

      auto fileLocationDecoded = "DecodeTable"_(std::move(saveTable));
      auto lazyLoadDataExpr =
          "GatherTables"_(RBL_PATH, std::move("f_file_location"_),
			  std::move(fileLocationDecoded), "List"_("List"_()),
			  "List"_("sample_value"_, "d_file_location"_));
      auto encodeLazyLoadData = "EncodeTable"_(std::move(lazyLoadDataExpr));

      auto getFilesSelect = "GetTable"_("FilesTable"_);

      auto dataFilesCatalogJoin =
          "Join"_(std::move(encodeLazyLoadData), std::move(getFilesSelect),
                  "Where"_("Equal"_("d_file_location"_, "f_file_location"_)));

      auto dataFilesCatalogProject = "Project"_(
          std::move(dataFilesCatalogJoin),
          "As"_("sample_value"_, "sample_value"_, "station"_, "station"_));
      auto groupBy =
          "Group"_(std::move(dataFilesCatalogProject), "By"_("station"_),
                   "As"_("avg_sample_value"_, "Avg"_("sample_value"_),
                         "sum_sample_value"_, "Sum"_("sample_value"_)));
      auto decodeGroupBy = "DecodeTable"_(std::move(groupBy));

      return std::move(decodeGroupBy);
    },
    [] {
      std::string dataFileURL = "https://www.doc.ic.ac.uk/~dcl19/2019-08-02-0200-00M.PNR02_HHE.bin";
      auto eagerLoadDataExpr = "Gather"_(std::move(dataFileURL), RBL_PATH, "List"_("List"_()),
				   "List"_("d_seq_no"_, "sample_time"_, "sample_value"_));
      auto encodeDataExpr = "EncodeTable"_(std::move(eagerLoadDataExpr));
      auto innerDataProject =
          "Project"_(std::move(encodeDataExpr),
                     "As"_("d_seq_no"_, "d_seq_no"_, "sample_time"_,
                           "sample_time"_, "sample_value"_, "sample_value"_));
      auto innerDataSelect =
          "Project"_("Select"_(std::move(innerDataProject),
                               "Where"_("Equal"_("d_seq_no"_, 0))),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_));

      return std::move(innerDataSelect);
    },
    [] {
      std::string dataFileURL = "https://www.doc.ic.ac.uk/~dcl19/2019-08-02-0200-00M.PNR04_HHN.bin_incorrectEndpoint.bin";
      auto eagerLoadDataExpr = "Gather"_(std::move(dataFileURL), RBL_PATH, "List"_("List"_()),
				   "List"_("d_seq_no"_, "sample_time"_, "sample_value"_));
      auto encodeDataExpr = "EncodeTable"_(std::move(eagerLoadDataExpr));
      auto innerDataProject =
          "Project"_(std::move(encodeDataExpr),
                     "As"_("d_seq_no"_, "d_seq_no"_, "sample_time"_,
                           "sample_time"_, "sample_value"_, "sample_value"_));
      auto innerDataSelect =
          "Project"_("Select"_(std::move(innerDataProject),
                               "Where"_("Equal"_("d_seq_no"_, 1))),
                     "As"_("sample_time"_, "sample_time"_, "sample_value"_,
                           "sample_value"_));

      return std::move(innerDataSelect);
    }};
} // namespace boss::benchmarks::LazyLoading::MiniseedQueries
