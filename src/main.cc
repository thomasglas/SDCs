// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <arrow/scalar.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <iostream>
#include <chrono>
#include <string>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include "sdc.h"

int qdTree_min_block_size = 10000;
bool verbose = false;
bool add_latency = false;

enum query_index{
  primary,
  column_partition,
  qdTree
};

void reset_sdc(){
  std::ifstream f("../data/metadata.json");
  json metadata_json = json::parse(f);
  for(auto& table: metadata_json["tables"]){
    for(auto& query: table["workload"]){
      for(auto& filter: query["filters"]){
        std::filesystem::remove(std::string(filter["booleanMask"]));
      }
    }
    table["workload"].clear();
    std::vector<int> erase_indexes;
    // remove qd tree index
    for(int i=0; i<table["indexes"].size(); i++){
      if(table["indexes"][i]["type"]=="qdTree"){
          // remove data blocks
          std::ifstream f(table["indexes"][i]["filePath"]);
          json index = json::parse(f);
          for(auto dataBlock: index["dataBlocks"]){
              std::filesystem::remove(std::string(dataBlock["filePath"]));
          }
          // remove index file
          std::filesystem::remove(std::string(table["indexes"][i]["filePath"]));
          // remove qd index
          erase_indexes.push_back(i);
      }
      else if(table["indexes"][i]["type"]=="columnPartition"){
          // remove data blocks
          std::ifstream f(table["indexes"][i]["filePath"]);
          json index = json::parse(f);
          for(auto dataBlock: index["dataBlocks"]){
              std::filesystem::remove(std::string(dataBlock["filePath"]));
          }
          // remove index file
          std::filesystem::remove(std::string(table["indexes"][i]["filePath"]));
          // remove columnPartition index
          erase_indexes.push_back(i);
      }
    }
    for(int i=erase_indexes.size()-1; i>=0; i--){
      table["indexes"].erase(erase_indexes[i]);
    }
  }
  // write out updated metadata
  std::ofstream o2("../data/metadata.json");
  o2 << std::setw(2) << metadata_json << std::endl;
}

void run_workload_1(int index){
  auto begin = std::chrono::high_resolution_clock::now();

  { // 575989
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("fare_amount", ">", "20");
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(index,5);
  }
  { // 120482
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("tip_amount", ">", "10");
    // table.filter("tip_amount", ">=", "fare_amount", true);
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(index,5);
  }
  { // 117956
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("fare_amount", ">", "20");
    table.filter("tip_amount", ">", "10");
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(index,5);
  }
  { // 1379502
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("tip_amount", "<", "2");
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(index,5);
  }
  { // 213742
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("fare_amount", "<", "5");
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(index,5);
  }
  { // 910655
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("VendorID", "<=", "1");
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(index,5);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
  printf("Time measured: %.3f seconds.\n", elapsed.count() * 1e-9);
}

void run_workload_2(int index){
  auto begin = std::chrono::high_resolution_clock::now();

  { // 17
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("improvement_surcharge", ">", "0");
    table.filter("tolls_amount", ">=", "10");
    table.filter("tolls_amount", "<=", "10");
    table.filter("airport_fee", ">", "0");
    table.projection({"airport_fee", "fare_amount", "tip_amount", "total_amount", "tolls_amount", "improvement_surcharge"});
    table.head(index,5);
  }
  { // 89936
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("trip_distance", ">", "10");
    table.filter("tip_amount", "<", "5");
    table.projection({"tip_amount", "total_amount"});
    table.head(index,5);
  }
  { // 1839059
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("mta_tax", ">", "0");
    table.filter("extra", ">", "0");
    table.projection({"mta_tax", "extra", "total_amount", "trip_distance"});
    table.head(index,5);
  }
  { // 2802897
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("congestion_surcharge", ">", "0");
    table.filter("congestion_surcharge", "<=", "5");
    table.projection({"PULocationID", "DOLocationID", "total_amount", "congestion_surcharge"});
    table.head(index,5);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
  printf("Time measured: %.3f seconds.\n", elapsed.count() * 1e-9);
}

void run_workload_3(int index){
  auto begin = std::chrono::high_resolution_clock::now();

  { // 2994311
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("total_amount", "<", "60");
    table.projection({"fare_amount", "tip_amount", "total_amount", "trip_distance"});
    table.head(index,5);
  }
  { // 2894157
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("trip_distance", "<=", "10");
    table.projection({"fare_amount", "tip_amount", "total_amount", "trip_distance"});
    table.head(index,5);
  }
  { // 401169
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("tip_amount", ">=", "5");
    table.projection({"fare_amount", "tip_amount", "total_amount", "trip_distance"});
    table.head(index,5);
  }
  { // 596484
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("fare_amount", ">=", "20");
    table.projection({"fare_amount", "tip_amount", "total_amount", "trip_distance"});
    table.head(index,5);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
  printf("Time measured: %.3f seconds.\n", elapsed.count() * 1e-9);
}

void run_workload_4(int index){
  auto begin = std::chrono::high_resolution_clock::now();

  { // 360655
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("total_amount", "<", "10");
    table.projection({"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
      "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
      "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"});
    table.head(index,5);
  }
  { // 1794426
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("total_amount", ">=", "10");
    table.filter("total_amount", "<", "20");
    table.projection({"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
      "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
      "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"});
    table.head(index,5);
  }
  { // 519528
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("total_amount", ">=", "20");
    table.filter("total_amount", "<", "30");
    table.projection({"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
      "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
      "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"});
    table.head(index,5);
  }
  { // 147278
    SDC::Dataframe table("NYCtaxi", add_latency, verbose);
    table.filter("total_amount", ">=", "30");
    table.filter("total_amount", "<", "40");
    table.projection({"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
      "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
      "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"});
    table.head(index,5);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
  printf("Time measured: %.3f seconds.\n", elapsed.count() * 1e-9);
}

void optimize(std::string column_partition, int min_leaf_size){
  SDC::Dataframe table("NYCtaxi", add_latency, verbose);
  table.optimize(column_partition, min_leaf_size);
}

void run_workload(int i){
  std::cout << "Workload " << i << std::endl;
  switch(i){
    case 1:{
      reset_sdc();
      run_workload_1(1);
      optimize("tip_amount", qdTree_min_block_size);
      run_workload_1(2);
      run_workload_1(3);
      break;
    }
    case 2:{
      reset_sdc();
      run_workload_2(1);
      optimize("improvement_surcharge", qdTree_min_block_size);
      run_workload_2(2);
      run_workload_2(3);
      break;
    }
    case 3:{
      reset_sdc();
      run_workload_3(1);
      optimize("fare_amount", qdTree_min_block_size);
      run_workload_3(2);
      run_workload_3(3);
      break;
    }
    case 4:{
      reset_sdc();
      run_workload_4(1);
      optimize("total_amount", qdTree_min_block_size);
      run_workload_4(2);
      run_workload_4(3);
      break;
    }
  }
}

int main(int argc, char** argv) {

  int workload = 1;

  if(argc>1){
    workload = std::stoi(argv[1]);
    qdTree_min_block_size = std::stoi(argv[2]);
    if(argc==4 && argv[3]==std::string("-v")){
      verbose = true;
    }
  }
  run_workload(workload);
  reset_sdc();
  
  return 0;
}