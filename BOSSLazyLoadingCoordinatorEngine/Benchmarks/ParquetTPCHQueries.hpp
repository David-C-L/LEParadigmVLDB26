#include "config.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <iostream>
#include <limits>
#include <duckdb.hpp>
#include "utilities.hpp"

#pragma once

namespace boss::benchmarks::LazyLoading::ParquetTPCHQueries {

using namespace std;
using namespace boss;
using utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using boss::benchmarks::LazyLoading::utilities::wrapEval;
using boss::benchmarks::LazyLoading::config::paths::RBL_PATH;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_1;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_10;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_100;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_1000;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_10000;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_20000;
using boss::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_100000;

enum TPCH_TABLE {
  TPCH_CUSTOMER = 0,
  TPCH_LINEITEM = 1,
  TPCH_NATION = 2,
  TPCH_ORDERS = 3,
  TPCH_PART = 4,
  TPCH_PARTSUPP = 5,
  TPCH_REGION = 6,
  TPCH_SUPPLIER = 7
};
  
  extern int64_t PARQUET_COMPRESSION;
  int64_t PARQUET_COMPRESSION = 0;

    std::string DATASET_BASE_URL = "http://maru02.doc.res.ic.ac.uk/files/";

std::string SF_1_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_customer.parquet";
std::string SF_1_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_lineitem.parquet";
std::string SF_1_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_nation.parquet";
std::string SF_1_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_orders.parquet";
std::string SF_1_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_part.parquet";
std::string SF_1_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_partsupp.parquet";
std::string SF_1_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_region.parquet";
std::string SF_1_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_1MB_supplier.parquet";

std::string SF_10_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_customer.parquet";
std::string SF_10_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_lineitem.parquet";
std::string SF_10_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_nation.parquet";
std::string SF_10_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_orders.parquet";
std::string SF_10_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_part.parquet";
std::string SF_10_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_partsupp.parquet";
std::string SF_10_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_region.parquet";
std::string SF_10_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_10MB_supplier.parquet";

std::string SF_100_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_customer.parquet";
std::string SF_100_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_lineitem.parquet";
std::string SF_100_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_nation.parquet";
std::string SF_100_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_orders.parquet";
std::string SF_100_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_part.parquet";
std::string SF_100_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_partsupp.parquet";
std::string SF_100_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_region.parquet";
std::string SF_100_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_100MB_supplier.parquet";

std::string SF_1000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_customer.parquet";
std::string SF_1000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_lineitem.parquet";
std::string SF_1000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_nation.parquet";
std::string SF_1000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_orders.parquet";
std::string SF_1000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_part.parquet";
std::string SF_1000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_partsupp.parquet";
std::string SF_1000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_region.parquet";
std::string SF_1000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_1000MB_supplier.parquet";

std::string SF_LIGHT_COMPRESSED_1000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_customer.parquet";
std::string SF_LIGHT_COMPRESSED_1000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_lineitem.parquet";
std::string SF_LIGHT_COMPRESSED_1000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_nation.parquet";
std::string SF_LIGHT_COMPRESSED_1000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_orders.parquet";
std::string SF_LIGHT_COMPRESSED_1000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_part.parquet";
std::string SF_LIGHT_COMPRESSED_1000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_partsupp.parquet";
std::string SF_LIGHT_COMPRESSED_1000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_region.parquet";
std::string SF_LIGHT_COMPRESSED_1000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_1000MB_supplier.parquet";

std::string SF_COMPRESSED_1000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_customer.parquet";
std::string SF_COMPRESSED_1000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_lineitem.parquet";
std::string SF_COMPRESSED_1000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_nation.parquet";
std::string SF_COMPRESSED_1000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_orders.parquet";
std::string SF_COMPRESSED_1000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_part.parquet";
std::string SF_COMPRESSED_1000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_partsupp.parquet";
std::string SF_COMPRESSED_1000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_region.parquet";
std::string SF_COMPRESSED_1000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_1000MB_supplier.parquet";

std::string SF_10000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_customer.parquet";
std::string SF_10000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_lineitem.parquet";
std::string SF_10000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_nation.parquet";
std::string SF_10000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_orders.parquet";
std::string SF_10000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_part.parquet";
std::string SF_10000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_partsupp.parquet";
std::string SF_10000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_region.parquet";
std::string SF_10000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_10000MB_supplier.parquet";

std::string SF_LIGHT_COMPRESSED_10000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_customer.parquet";
std::string SF_LIGHT_COMPRESSED_10000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_lineitem.parquet";
std::string SF_LIGHT_COMPRESSED_10000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_nation.parquet";
std::string SF_LIGHT_COMPRESSED_10000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_orders.parquet";
std::string SF_LIGHT_COMPRESSED_10000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_part.parquet";
std::string SF_LIGHT_COMPRESSED_10000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_partsupp.parquet";
std::string SF_LIGHT_COMPRESSED_10000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_region.parquet";
std::string SF_LIGHT_COMPRESSED_10000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_10000MB_supplier.parquet";

std::string SF_COMPRESSED_10000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_customer.parquet";
std::string SF_COMPRESSED_10000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_lineitem.parquet";
std::string SF_COMPRESSED_10000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_nation.parquet";
std::string SF_COMPRESSED_10000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_orders.parquet";
std::string SF_COMPRESSED_10000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_part.parquet";
std::string SF_COMPRESSED_10000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_partsupp.parquet";
std::string SF_COMPRESSED_10000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_region.parquet";
std::string SF_COMPRESSED_10000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_10000MB_supplier.parquet";

std::string SF_20000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_customer.parquet";
std::string SF_20000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_lineitem.parquet";
std::string SF_20000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_nation.parquet";
std::string SF_20000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_orders.parquet";
std::string SF_20000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_part.parquet";
std::string SF_20000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_partsupp.parquet";
std::string SF_20000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_region.parquet";
std::string SF_20000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_20000MB_supplier.parquet";

std::string SF_LIGHT_COMPRESSED_20000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_customer.parquet";
std::string SF_LIGHT_COMPRESSED_20000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_lineitem.parquet";
std::string SF_LIGHT_COMPRESSED_20000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_nation.parquet";
std::string SF_LIGHT_COMPRESSED_20000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_orders.parquet";
std::string SF_LIGHT_COMPRESSED_20000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_part.parquet";
std::string SF_LIGHT_COMPRESSED_20000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_partsupp.parquet";
std::string SF_LIGHT_COMPRESSED_20000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_region.parquet";
std::string SF_LIGHT_COMPRESSED_20000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_20000MB_supplier.parquet";

std::string SF_COMPRESSED_20000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_customer.parquet";
std::string SF_COMPRESSED_20000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_lineitem.parquet";
std::string SF_COMPRESSED_20000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_nation.parquet";
std::string SF_COMPRESSED_20000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_orders.parquet";
std::string SF_COMPRESSED_20000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_part.parquet";
std::string SF_COMPRESSED_20000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_partsupp.parquet";
std::string SF_COMPRESSED_20000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_region.parquet";
std::string SF_COMPRESSED_20000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_20000MB_supplier.parquet";

  std::string SF_LIGHT_COMPRESSED_100000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_customer.parquet";
std::string SF_LIGHT_COMPRESSED_100000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_lineitem.parquet";
std::string SF_LIGHT_COMPRESSED_100000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_nation.parquet";
std::string SF_LIGHT_COMPRESSED_100000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_orders.parquet";
std::string SF_LIGHT_COMPRESSED_100000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_part.parquet";
std::string SF_LIGHT_COMPRESSED_100000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_partsupp.parquet";
std::string SF_LIGHT_COMPRESSED_100000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_region.parquet";
std::string SF_LIGHT_COMPRESSED_100000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_light_compressed_100000MB_supplier.parquet";

std::string SF_COMPRESSED_100000_CUSTOMER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_customer.parquet";
std::string SF_COMPRESSED_100000_LINEITEM_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_lineitem.parquet";
std::string SF_COMPRESSED_100000_NATION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_nation.parquet";
std::string SF_COMPRESSED_100000_ORDERS_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_orders.parquet";
std::string SF_COMPRESSED_100000_PART_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_part.parquet";
std::string SF_COMPRESSED_100000_PARTSUPP_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_partsupp.parquet";
std::string SF_COMPRESSED_100000_REGION_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_region.parquet";
std::string SF_COMPRESSED_100000_SUPPLIER_TABLE_URL =
    DATASET_BASE_URL + "tpch_compressed_100000MB_supplier.parquet";

std::unordered_map<TPCH_TABLE, std::unordered_map<TPCH_SF, std::string>>
    tableURLs{{TPCH_CUSTOMER,
               {
                   {TPCH_SF_1, SF_1_CUSTOMER_TABLE_URL},
                   {TPCH_SF_10, SF_10_CUSTOMER_TABLE_URL},
                   {TPCH_SF_100, SF_100_CUSTOMER_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_CUSTOMER_TABLE_URL},
               }},
              {TPCH_LINEITEM,
               {
                   {TPCH_SF_1, SF_1_LINEITEM_TABLE_URL},
                   {TPCH_SF_10, SF_10_LINEITEM_TABLE_URL},
                   {TPCH_SF_100, SF_100_LINEITEM_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_LINEITEM_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_LINEITEM_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_LINEITEM_TABLE_URL},
               }},
              {TPCH_NATION,
               {
                   {TPCH_SF_1, SF_1_NATION_TABLE_URL},
                   {TPCH_SF_10, SF_10_NATION_TABLE_URL},
                   {TPCH_SF_100, SF_100_NATION_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_NATION_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_NATION_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_NATION_TABLE_URL},
               }},
              {TPCH_ORDERS,
               {
                   {TPCH_SF_1, SF_1_ORDERS_TABLE_URL},
                   {TPCH_SF_10, SF_10_ORDERS_TABLE_URL},
                   {TPCH_SF_100, SF_100_ORDERS_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_ORDERS_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_ORDERS_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_ORDERS_TABLE_URL},
               }},
              {TPCH_PART,
               {
                   {TPCH_SF_1, SF_1_PART_TABLE_URL},
                   {TPCH_SF_10, SF_10_PART_TABLE_URL},
                   {TPCH_SF_100, SF_100_PART_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_PART_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_PART_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_PART_TABLE_URL},
               }},
              {TPCH_PARTSUPP,
               {
                   {TPCH_SF_1, SF_1_PARTSUPP_TABLE_URL},
                   {TPCH_SF_10, SF_10_PARTSUPP_TABLE_URL},
                   {TPCH_SF_100, SF_100_PARTSUPP_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_PARTSUPP_TABLE_URL},
               }},
              {TPCH_REGION,
               {
                   {TPCH_SF_1, SF_1_REGION_TABLE_URL},
                   {TPCH_SF_10, SF_10_REGION_TABLE_URL},
                   {TPCH_SF_100, SF_100_REGION_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_REGION_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_REGION_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_REGION_TABLE_URL},
               }},
              {TPCH_SUPPLIER,
               {
                   {TPCH_SF_1, SF_1_SUPPLIER_TABLE_URL},
                   {TPCH_SF_10, SF_10_SUPPLIER_TABLE_URL},
                   {TPCH_SF_100, SF_100_SUPPLIER_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_SUPPLIER_TABLE_URL},
               }}};
  
std::unordered_map<TPCH_TABLE, std::unordered_map<TPCH_SF, std::string>>
     lightCompressedTableURLs{{TPCH_CUSTOMER,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_CUSTOMER_TABLE_URL},
	       }},
              {TPCH_LINEITEM,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_LINEITEM_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_LINEITEM_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_LINEITEM_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_LINEITEM_TABLE_URL},
               }},
              {TPCH_NATION,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_NATION_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_NATION_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_NATION_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_NATION_TABLE_URL},
               }},
              {TPCH_ORDERS,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_ORDERS_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_ORDERS_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_ORDERS_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_ORDERS_TABLE_URL},
               }},
              {TPCH_PART,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_PART_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_PART_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_PART_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_PART_TABLE_URL},
               }},
              {TPCH_PARTSUPP,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_PARTSUPP_TABLE_URL},
               }},
              {TPCH_REGION,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_REGION_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_REGION_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_REGION_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_REGION_TABLE_URL},
               }},
              {TPCH_SUPPLIER,
               {
                   {TPCH_SF_1000, SF_LIGHT_COMPRESSED_1000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_10000, SF_LIGHT_COMPRESSED_10000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_20000, SF_LIGHT_COMPRESSED_20000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_100000, SF_LIGHT_COMPRESSED_100000_SUPPLIER_TABLE_URL},
               }}};
  
std::unordered_map<TPCH_TABLE, std::unordered_map<TPCH_SF, std::string>>
     compressedTableURLs{{TPCH_CUSTOMER,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_CUSTOMER_TABLE_URL},
	       }},
              {TPCH_LINEITEM,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_LINEITEM_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_LINEITEM_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_LINEITEM_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_LINEITEM_TABLE_URL},
               }},
              {TPCH_NATION,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_NATION_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_NATION_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_NATION_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_NATION_TABLE_URL},
               }},
              {TPCH_ORDERS,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_ORDERS_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_ORDERS_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_ORDERS_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_ORDERS_TABLE_URL},
               }},
              {TPCH_PART,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_PART_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_PART_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_PART_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_PART_TABLE_URL},
               }},
              {TPCH_PARTSUPP,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_PARTSUPP_TABLE_URL},
               }},
              {TPCH_REGION,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_REGION_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_REGION_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_REGION_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_REGION_TABLE_URL},
               }},
              {TPCH_SUPPLIER,
               {
                   {TPCH_SF_1000, SF_COMPRESSED_1000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_10000, SF_COMPRESSED_10000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_20000, SF_COMPRESSED_20000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_100000, SF_COMPRESSED_100000_SUPPLIER_TABLE_URL},
               }}};

  inline int32_t runDuckDBQuery(duckdb::Connection &con, std::string query) {
  try {
    std::cout << query << std::endl;
    auto result = con.Query(query);

    if (!result) {
      std::cerr << "DuckDB query failed: " << result->GetError() << std::endl;
      return 1;
    }
    std::cout << result->ToString() << std::endl;
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "DuckDB Error: " << e.what() << std::endl;
    return 1;
  }
}

inline boss::Expression createSingleList(std::vector<boss::Symbol> symbols) {
  boss::ExpressionArguments args;
  for (boss::Symbol symbol : symbols) {
    args.push_back(symbol);
  }
  auto list = boss::ComplexExpression{"List"_, {}, std::move(args), {}};
  return std::move(list);
}

inline boss::Expression
createIndicesAsNoRename(std::vector<boss::Symbol> symbols) {
  boss::ExpressionArguments args;
  args.push_back("__internal_indices_"_);
  args.push_back("__internal_indices_"_);
  for (boss::Symbol symbol : symbols) {
    args.push_back(symbol);
    args.push_back(symbol);
  }
  auto as = boss::ComplexExpression{"As"_, {}, std::move(args), {}};
  return std::move(as);
}

inline boss::Expression
getGatherSelectGatherWrap(std::string url, boss::Expression &&gatherIndices1,
                      std::vector<boss::Symbol> gatherColumns1,
                      boss::Expression &&where,
			  std::vector<boss::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2) {
  auto const gather1 = wrapEval("Gather"_(url, RBL_PATH, std::move(gatherIndices1),
					  std::move(createSingleList(gatherColumns1))), s1);
  auto encoded1 = std::move(gather1.clone(expressions::CloneReason::FOR_TESTING));
  if (encode1) {
    encoded1 = wrapEval("EncodeTable"_(std::move(gather1.clone(expressions::CloneReason::FOR_TESTING))), s1);
  }
  auto project = wrapEval("Project"_("AddIndices"_(std::move(encoded1), "__internal_indices_"_),
				     std::move(createIndicesAsNoRename(gatherColumns1))), s1);
  auto indices = wrapEval("Project"_(wrapEval("Select"_(std::move(project), std::move(where)), s1),
				     std::move(createIndicesAsNoRename({}))), s1);
  auto gather2 = wrapEval("Gather"_(url, RBL_PATH, std::move(indices),
				    std::move(createSingleList(gatherColumns2))), s2);
  if (encode2) {
    auto encoded2 = wrapEval("EncodeTable"_(std::move(gather2)), s2);
    return std::move(encoded2);
  }

  return std::move(gather2);
}
  
inline boss::Expression
getSelectGatherWrap(std::string url, boss::Expression &&table,
                      std::vector<boss::Symbol> tableColumns,
                      boss::Expression &&where,
		    std::vector<boss::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2) {
  auto project = wrapEval("Project"_(std::move(table),
				     std::move(createIndicesAsNoRename(tableColumns))), s1);
  auto indices = wrapEval("Project"_(wrapEval("Select"_(std::move(project), std::move(where)), s1),
				     std::move(createIndicesAsNoRename({}))), s1);
  auto gather2 = wrapEval("Gather"_(url, RBL_PATH, std::move(indices),
				    std::move(createSingleList(gatherColumns2))), s2);
  if (encode2) {
    auto encoded2 = wrapEval("EncodeTable"_(std::move(gather2)), s2);
    return std::move(encoded2);
  }

  return std::move(gather2);
}
  
inline boss::Expression TPCH_Q1_BOSS_CYCLE_PARQUET(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto url = tpchTableMap[TPCH_LINEITEM][sf];
  auto getParquetColsExpr = wrapEval("GetColumnsFromParquet"_(RBL_PATH, url, "List"_("l_shipdate"_("ALL"_()), "l_returnflag"_("ALL"_()), "l_linestatus"_("ALL"_()), "l_tax"_("ALL"_()), "l_discount"_("ALL"_()), "l_extendedprice"_("ALL"_()), "l_quantity"_("ALL"_()))), 0);
  auto encodedParquetCols = wrapEval("EncodeTable"_(std::move(getParquetColsExpr)), 0);
  
  auto select = 
    wrapEval("Select"_(
		       wrapEval("Project"_(std::move(encodedParquetCols),
			 "As"_("l_quantity"_, "l_quantity"_,
			       "l_discount"_, "l_discount"_,
			       "l_shipdate"_, "l_shipdate"_,
			       "l_extendedprice"_,
			       "l_extendedprice"_,
			       "l_returnflag"_, "l_returnflag"_,
			       "l_linestatus"_, "l_linestatus"_,
			       "l_tax"_, "l_tax"_)), 0),
	      "Where"_("Greater"_("DateObject"_("1998-08-31"),
				  "l_shipdate"_))), 0);

  auto query = wrapEval("Order"_(
      wrapEval("Project"_(
          wrapEval("Group"_(
              wrapEval("Project"_(
                  wrapEval("Project"_(
                      wrapEval("Project"_(std::move(select),
                                 "As"_("l_returnflag"_, "l_returnflag"_,
                                       "l_linestatus"_, "l_linestatus"_,
                                       "l_quantity"_, "l_quantity"_,
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_discount"_, "l_discount"_, "calc1"_,
                                       "Minus"_(1.0, "l_discount"_), "calc2"_,
                                       "Plus"_("l_tax"_, 1.0))), 0),
                      "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                            "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "disc_price"_,
                            "Times"_("l_extendedprice"_, "calc1"_), "calc2"_,
                            "calc2"_)), 0),
                  "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                        "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                        "l_extendedprice"_, "l_extendedprice"_, "l_discount"_,
                        "l_discount"_, "disc_price"_, "disc_price"_, "calc"_,
                        "Times"_("disc_price"_, "calc2"_))), 0),
              "By"_("l_returnflag"_, "l_linestatus"_),
              "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                    "Sum"_("l_extendedprice"_), "sum_disc_price"_,
                    "Sum"_("disc_price"_), "sum_charges"_, "Sum"_("calc"_),
                    "sum_disc"_, "Sum"_("l_discount"_), "count_order"_,
                    "Count"_("*"_))), 0),
          "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                "l_linestatus"_, "sum_qty"_, "sum_qty"_, "sum_base_price"_,
                "sum_base_price"_, "sum_disc_price"_, "sum_disc_price"_,
                "sum_charges"_, "sum_charges"_, "avg_qty"_,
                "Divide"_("sum_qty"_, "count_order"_), "avg_price"_,
                "Divide"_("sum_base_price"_, "count_order"_), "avg_disc"_,
                "Divide"_("sum_disc"_, "count_order"_), "count_order"_,
                "count_order"_)), 0),
      "By"_("l_returnflag"_, "l_linestatus"_)), 0);

  return std::move(query);
}

inline boss::Expression TPCH_Q3_BOSS_CYCLE_PARQUET(TPCH_SF sf) {
   auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : compressedTableURLs;
   auto customerUrl = tpchTableMap[TPCH_CUSTOMER][sf];
   auto customerParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, customerUrl, "List"_("c_mktsegment"_("ALL"_()), "c_custkey"_("ALL"_()))), 0);
   auto encodeCustomerParquetCols = wrapEval("EncodeTable"_(std::move(customerParquetCols)), 0);
   
   auto ordersUrl = tpchTableMap[TPCH_ORDERS][sf];
   auto ordersParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, ordersUrl, "List"_("o_orderkey"_("ALL"_()), "o_custkey"_("ALL"_()), "o_orderdate"_("ALL"_()), "o_shippriority"_("ALL"_()))), 0);
   
   auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
   auto lineitemParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, lineitemUrl, "List"_("l_shipdate"_("ALL"_()), "l_orderkey"_("ALL"_()), "l_extendedprice"_("ALL"_()), "l_discount"_("ALL"_()))), 0);

   auto orderSelect =
     wrapEval("Select"_(
			wrapEval("Project"_(std::move(ordersParquetCols),
					    "As"_("o_orderkey"_, "o_orderkey"_,
						  "o_orderdate"_, "o_orderdate"_,
						  "o_custkey"_, "o_custkey"_,
						  "o_shippriority"_,
						  "o_shippriority"_)), 0),
			"Where"_("Greater"_("DateObject"_("1995-03-15"),
					    "o_orderdate"_))), 0);

   auto customerSelect =
     wrapEval("Project"_(
			 wrapEval("Select"_(
					    wrapEval("Project"_(std::move(encodeCustomerParquetCols),
								"As"_("c_custkey"_, "c_custkey"_,
								      "c_mktsegment"_,
								      "c_mktsegment"_)), 0),
					    "Where"_("Equal"_("c_mktsegment"_,
							      "GetEncodingFor"_("BUILDING", "c_mktsegment"_)))), 0),
			 "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_,
			       "c_mktsegment"_)), 0);

   auto lineitemSelect =
     wrapEval("Project"_(
			 wrapEval("Select"_(
					    wrapEval("Project"_(
								std::move(lineitemParquetCols),
								"As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
								      "l_discount"_, "l_shipdate"_, "l_shipdate"_,
								      "l_extendedprice"_, "l_extendedprice"_)), 0),
						     "Where"_("Greater"_("l_shipdate"_,
									 "DateObject"_("1995-03-15")))), 0),
					    "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
						  "l_discount"_, "l_extendedprice"_,
						  "l_extendedprice"_)), 0);
   
  auto ordersCustomerJoin = wrapEval("Project"_(
      wrapEval("Join"_(std::move(orderSelect), std::move(customerSelect),
		       "Where"_("Equal"_("o_custkey"_, "c_custkey"_))), 0),
      "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
            "o_shippriority"_, "o_shippriority"_)), 0);

  auto ordersLineitemJoin = wrapEval("Project"_(
      wrapEval("Join"_(std::move(ordersCustomerJoin), std::move(lineitemSelect),
		       "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 0),
      "As"_("l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
            "l_extendedprice"_, "l_discount"_, "l_discount"_, "o_orderdate"_,
            "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)), 0);
  
  auto query =
      wrapEval("Top"_(wrapEval("Group"_(wrapEval("Project"_(std::move(ordersLineitemJoin),
                                 "As"_("expr1009"_,
                                       "Times"_("l_extendedprice"_,
                                                "Minus"_(1.0, "l_discount"_)),
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_orderkey"_, "l_orderkey"_,
                                       "o_orderdate"_, "o_orderdate"_,
                                       "o_shippriority"_, "o_shippriority"_)), 0),
                      "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
					"As"_("revenue"_, "Sum"_("expr1009"_))), 0),
		      "By"_("revenue"_, "desc"_, "o_orderdate"_), 10), 0);

  return std::move(query);
}

inline boss::Expression TPCH_Q6_BOSS_CYCLE_PARQUET(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto url = tpchTableMap[TPCH_LINEITEM][sf];
  auto getParquetColsExpr = wrapEval("GetColumnsFromParquet"_(RBL_PATH, url, "List"_("l_shipdate"_("ALL"_()), "l_discount"_("List"_((double_t) 0.0499, (double_t) 0.07001)), "l_extendedprice"_("ALL"_()), "l_quantity"_("List"_(std::numeric_limits<double_t>::min(), (double_t) 23)))), 0);

  auto project1 =
    wrapEval("Project"_(std::move(getParquetColsExpr),
			"As"_("l_discount"_, "l_discount"_,
			      "l_extendedprice"_, "l_extendedprice"_,
			      "l_quantity"_, "l_quantity"_,
			      "l_shipdate"_, "l_shipdate"_,
			      "__internal_indices_"_, "__internal_indices_"_)), 0);
  auto select1 =
    wrapEval("Select"_(std::move(project1),
		       "Where"_("And"_("Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_), // NOLINT
				       "Greater"_("l_shipdate", "DateObject"_("1993-12-31"))))), 0);

  auto select2 =
    wrapEval("Select"_(std::move(select1),
		       "Where"_("Greater"_((double_t) 24, "l_quantity"_))), 1);
  auto select3 =
    wrapEval("Select"_(std::move(select2),
		       "Where"_("And"_("Greater"_("l_discount"_, 0.0499), // NOLINT
				       "Greater"_(0.07001, "l_discount"_)))), 2);
  auto project2 = wrapEval("Project"_(std::move(select3),
				      "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))), 3);
  
  auto query = wrapEval("Group"_(std::move(project2),
      "Sum"_("revenue"_)), 4);

  return std::move(query);
}

inline boss::Expression TPCH_Q9_BOSS_CYCLE_PARQUET(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto partUrl = tpchTableMap[TPCH_PART][sf];
  auto partParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, partUrl, "List"_("p_partkey"_("ALL"_()), "p_retailprice"_("List"_((double_t) 1006.05, (double_t) 1080.1)))), 0);
  
  auto orderUrl = tpchTableMap[TPCH_ORDERS][sf];
  auto orderParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, orderUrl, "List"_("o_orderkey"_("ALL"_()), "o_orderdate"_("ALL"_()))), 0);
  
  auto nationUrl = tpchTableMap[TPCH_NATION][sf];
  auto nationParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, nationUrl, "List"_("n_nationkey"_("ALL"_()), "n_name"_("ALL"_()))), 0);
  
  auto supplierUrl = tpchTableMap[TPCH_SUPPLIER][sf];
  auto supplierParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, supplierUrl, "List"_("s_suppkey"_("ALL"_()), "s_nationkey"_("ALL"_()))), 0);
  
  auto partsuppUrl = tpchTableMap[TPCH_PARTSUPP][sf];
  auto partsuppParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, partsuppUrl, "List"_("ps_suppkey"_("ALL"_()), "ps_partkey"_("ALL"_()), "ps_supplycost"_("ALL"_()))), 0);
  
  auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
  auto lineitemParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, lineitemUrl, "List"_("l_suppkey"_("ALL"_()), "l_partkey"_("ALL"_()), "l_quantity"_("ALL"_()), "l_orderkey"_("ALL"_()), "l_extendedprice"_("ALL"_()), "l_discount"_("ALL"_()))), 0);

  auto nationEncoded = wrapEval("EncodeTable"_(std::move(nationParquetCols)), 0);

  auto query = wrapEval("Order"_(
      wrapEval("Group"_(
          wrapEval("Project"_(
              wrapEval("Join"_(
                  wrapEval("Project"_(std::move(orderParquetCols),
                             "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                   "o_orderdate"_)), 0),
                  wrapEval("Project"_(
                      wrapEval("Join"_(
                          wrapEval("Project"_(
                              wrapEval("Join"_(
                                  wrapEval("Project"_(std::move(partParquetCols),
						      "As"_("p_partkey"_, "p_partkey"_)), 0),
                                  wrapEval("Project"_(
                                      wrapEval("Join"_(
                                          wrapEval("Project"_(
                                              wrapEval("Join"_(
                                                  wrapEval("Project"_(
                                                      std::move(nationEncoded),
                                                      "As"_("n_name"_,
                                                            "n_name"_,
                                                            "n_nationkey"_,
                                                            "n_nationkey"_)), 0),
                                                  wrapEval("Project"_(
                                                      std::move(supplierParquetCols),
                                                      "As"_("s_suppkey"_,
                                                            "s_suppkey"_,
                                                            "s_nationkey"_,
                                                            "s_nationkey"_)), 0),
                                                  "Where"_("Equal"_(
                                                      "n_nationkey"_,
                                                      "s_nationkey"_))), 0),
                                              "As"_("n_name"_, "n_name"_,
                                                    "s_suppkey"_,
                                                    "s_suppkey"_)), 0),
                                          wrapEval("Project"_(std::move(partsuppParquetCols),
                                                     "As"_("ps_partkey"_,
                                                           "ps_partkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_supplycost"_,
                                                           "ps_supplycost"_)), 0),
                                          "Where"_("Equal"_("s_suppkey"_,
                                                            "ps_suppkey"_))), 0),
                                      "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                            "ps_partkey"_, "ps_suppkey"_,
                                            "ps_suppkey"_, "ps_supplycost"_,
                                            "ps_supplycost"_)), 0),
                                  "Where"_(
					   "Equal"_("p_partkey"_, "ps_partkey"_))), 0),
                              "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                    "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                    "ps_supplycost"_, "ps_supplycost"_)), 0),
                          wrapEval("Project"_(
                              std::move(lineitemParquetCols),
                              "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                    "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                    "l_extendedprice"_, "l_extendedprice"_,
                                    "l_discount"_, "l_discount"_, "l_quantity"_,
                                    "l_quantity"_)), 0),
                          "Where"_(
                              "Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                       "List"_("l_partkey"_, "l_suppkey"_)))), 0),
                      "As"_("n_name"_, "n_name"_, "ps_supplycost"_,
                            "ps_supplycost"_, "l_orderkey"_, "l_orderkey"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "l_quantity"_,
                            "l_quantity"_)), 0),
                  "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 0),
              "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_),
                    "amount"_,
                    "Minus"_("Times"_("l_extendedprice"_,
                                      "Minus"_(1.0, "l_discount"_)),
                             "Times"_("ps_supplycost"_, "l_quantity"_)))), 0),
          "By"_("nation"_, "o_year"_), "Sum"_("amount"_)), 0),
      "By"_("nation"_, "o_year"_, "desc"_)), 0);

  return std::move(query);
}

inline boss::Expression TPCH_Q18_BOSS_CYCLE_PARQUET(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto customerUrl = tpchTableMap[TPCH_CUSTOMER][sf];
  auto customerParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, customerUrl, "List"_("c_custkey"_("ALL"_()))), 0);
  
  auto orderUrl = tpchTableMap[TPCH_ORDERS][sf];
  auto orderParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, orderUrl, "List"_("o_orderkey"_("ALL"_()), "o_orderdate"_("ALL"_()), "o_totalprice"_("ALL"_()), "o_custkey"_("ALL"_()))), 0);

  auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
  auto lineitemParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, lineitemUrl, "List"_("l_quantity"_("ALL"_()), "l_orderkey"_("ALL"_()))), 0);

  auto query = wrapEval("Top"_(
      wrapEval("Group"_(
          wrapEval("Project"_(
              wrapEval("Join"_(
                  wrapEval("Select"_(
                      wrapEval("Group"_(wrapEval("Project"_(std::move(lineitemParquetCols),
                                          "As"_("l_orderkey"_, "l_orderkey"_,
                                                "l_quantity"_, "l_quantity"_)), 0),
					"By"_("l_orderkey"_),
					"As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))), 0),
                      "Where"_("Greater"_("sum_l_quantity"_, (double_t) 300))), 0), // NOLINT
                  wrapEval("Project"_(
                      wrapEval("Join"_(
                          wrapEval("Project"_(std::move(customerParquetCols),
					      "As"_("c_custkey"_, "c_custkey"_)), 0),
                          wrapEval("Project"_(std::move(orderParquetCols),
                                     "As"_("o_orderkey"_, "o_orderkey"_,
                                           "o_custkey"_, "o_custkey"_,
                                           "o_orderdate"_, "o_orderdate"_,
                                           "o_totalprice"_, "o_totalprice"_)), 0),
                          "Where"_("Equal"_("c_custkey"_, "o_custkey"_))), 0),
                      "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_,
                            "o_custkey"_, "o_orderdate"_, "o_orderdate"_,
                            "o_totalprice"_, "o_totalprice"_)), 0),
                  "Where"_("Equal"_("l_orderkey"_, "o_orderkey"_))), 0),
              "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                    "o_orderdate"_, "o_totalprice"_, "o_totalprice"_,
                    "o_custkey"_, "o_custkey"_, "sum_l_quantity"_,
                    "sum_l_quantity"_)), 0),
          "By"_("o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_totalprice"_),
          "Sum"_("sum_l_quantity"_)), 0),
      "By"_("o_totalprice"_, "desc"_, "o_orderdate"_), 100), 0);

  return std::move(query);
}

inline std::string TPCH_Q1_DUCKDB(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
  std::ostringstream finalQuery;
  finalQuery << "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) + (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order ";
  finalQuery << "FROM read_parquet(\"" << lineitemUrl << "\") lineitem ";
  finalQuery << "WHERE l_shipdate <= DATE '1998-08-31' ";
  finalQuery << "GROUP BY l_returnflag, l_linestatus ";
  finalQuery << "ORDER BY l_returnflag, l_linestatus;";

  return finalQuery.str();
}
  
inline std::string TPCH_Q3_DUCKDB(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
  auto customerUrl = tpchTableMap[TPCH_CUSTOMER][sf];
  auto ordersUrl = tpchTableMap[TPCH_ORDERS][sf];

  std::ostringstream finalQuery;
  finalQuery << "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority ";
  finalQuery << "FROM read_parquet(\"" << lineitemUrl << "\") lineitem ";
  finalQuery << "JOIN read_parquet(\"" << ordersUrl << "\") orders ON (l_orderkey = o_orderkey) ";
  finalQuery << "JOIN read_parquet(\"" << customerUrl << "\") customer ON (o_custkey = c_custkey) ";
  finalQuery << "WHERE o_orderdate <= DATE '1995-03-15' ";
  finalQuery << "AND l_shipdate > DATE '1995-03-15' ";
  finalQuery << "AND c_mktsegment = 'BUILDING' ";
  finalQuery << "GROUP BY l_orderkey, o_orderdate, o_shippriority ";
  finalQuery << "ORDER BY revenue DESC, o_orderdate ";
  finalQuery << "LIMIT 20;";

  return finalQuery.str();
}

inline std::string TPCH_Q6_DUCKDB(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
  std::ostringstream finalQuery;
  finalQuery << "SELECT SUM(l_extendedprice * l_discount) AS revenue ";
  finalQuery << "FROM read_parquet(\"" << lineitemUrl << "\") lineitem ";
  finalQuery << "WHERE l_shipdate <= DATE '1995-01-01' ";
  finalQuery << "AND l_shipdate > DATE '1993-12-31' ";
  finalQuery << "AND l_quantity < 24 ";
  finalQuery << "AND l_discount > 0.0499 ";
  finalQuery << "AND l_discount < 0.07001;";
  return finalQuery.str();
}
 
inline std::string TPCH_Q9_DUCKDB(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
  auto partUrl = tpchTableMap[TPCH_PART][sf];
  auto partsuppUrl = tpchTableMap[TPCH_PARTSUPP][sf];
  auto supplierUrl = tpchTableMap[TPCH_SUPPLIER][sf];
  auto nationUrl = tpchTableMap[TPCH_NATION][sf];
  auto ordersUrl = tpchTableMap[TPCH_ORDERS][sf];
  
  std::ostringstream innerQuery;
  innerQuery << "SELECT n_name AS nation, extract('year' FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount ";
  innerQuery << "FROM read_parquet(\'" << lineitemUrl << "\') lineitem ";
  innerQuery << "JOIN read_parquet(\'" << ordersUrl << "\') orders ON (l_orderkey = o_orderkey) ";
  innerQuery << "JOIN read_parquet(\'" << partUrl << "\') part ON (p_partkey = l_partkey) ";
  innerQuery << "JOIN read_parquet(\'" << partsuppUrl << "\') partsupp ON (ps_partkey = l_partkey AND ps_suppkey = l_suppkey) ";
  innerQuery << "JOIN read_parquet(\'" << supplierUrl << "\') supplier ON (s_suppkey = l_suppkey) ";
  innerQuery << "JOIN read_parquet(\'" << nationUrl << "\') nation ON (s_nationkey = n_nationkey) ";
  innerQuery << "WHERE p_retailprice > 1006.05 ";
  innerQuery << "AND p_retailprice < 1080.1";

  std::ostringstream finalQuery;
  finalQuery << "SELECT nation, o_year, SUM(amount) AS sum_profit ";
  finalQuery << "FROM (" << innerQuery.str() << ") AS profit ";
  finalQuery << "GROUP BY nation, o_year ";
  finalQuery << "ORDER BY nation, o_year DESC ";
  finalQuery << "LIMIT 1;";

  return finalQuery.str();
}
  
inline std::string TPCH_Q18_DUCKDB(TPCH_SF sf) {
  auto& tpchTableMap = PARQUET_COMPRESSION == 0 ? tableURLs : (PARQUET_COMPRESSION == 1 ? lightCompressedTableURLs : compressedTableURLs);
  auto lineitemUrl = tpchTableMap[TPCH_LINEITEM][sf];
  auto customerUrl = tpchTableMap[TPCH_CUSTOMER][sf];
  auto ordersUrl = tpchTableMap[TPCH_ORDERS][sf];

  std::ostringstream innerQuery;
  innerQuery << "SELECT l_orderkey ";
  innerQuery << "FROM read_parquet(\"" << lineitemUrl << "\") lineitem ";
  innerQuery << "GROUP BY l_orderkey HAVING SUM(l_quantity) > 300";

  std::ostringstream finalQuery;
  finalQuery << "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) ";
  finalQuery << "FROM read_parquet(\"" << lineitemUrl << "\") lineitem ";
  finalQuery << "JOIN read_parquet(\"" << ordersUrl << "\") orders ON (l_orderkey = o_orderkey) ";
  finalQuery << "JOIN read_parquet(\"" << customerUrl << "\") customer ON (o_custkey = c_custkey) ";
  finalQuery << "WHERE o_orderkey IN (" << innerQuery.str() << ") ";
  finalQuery << "GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ";
  finalQuery << "ORDER BY o_totalprice DESC, o_orderdate ";
  finalQuery << "LIMIT 100;";
  
  return finalQuery.str();
}
  
inline std::vector<std::function<boss::Expression(TPCH_SF)>> bossParquetCycleQueriesTPCH{
    TPCH_Q1_BOSS_CYCLE_PARQUET, TPCH_Q3_BOSS_CYCLE_PARQUET, TPCH_Q6_BOSS_CYCLE_PARQUET, TPCH_Q9_BOSS_CYCLE_PARQUET, TPCH_Q18_BOSS_CYCLE_PARQUET};
  
  inline std::vector<std::function<std::string(TPCH_SF)>> duckDBQueriesTPCH{
    TPCH_Q1_DUCKDB, TPCH_Q3_DUCKDB, TPCH_Q6_DUCKDB, TPCH_Q9_DUCKDB, TPCH_Q18_DUCKDB};
} // namespace boss::benchmarks::LazyLoading::ParquetTPCHQueries
