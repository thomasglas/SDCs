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

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include "sdc.h"

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
          table["indexes"].erase(i);
          break;
      }
    }
  }
  // write out updated metadata
  std::ofstream o2("../data/metadata.json");
  o2 << std::setw(2) << metadata_json << std::endl;
}

void run_workload(){
  auto begin = std::chrono::high_resolution_clock::now();

  {
    SDC::Dataframe table("NYCtaxi");
    table.filter("VendorID", "<", "2");
    table.filter("fare_amount", ">", "10");
    // table.filter("tip_amount", "<", "10");
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(5);
  }
  {
    SDC::Dataframe table("NYCtaxi");
    table.filter("tip_amount", "<", "10");
    table.filter("tip_amount", ">", "5");
    // // table.filter("tip_amount", ">=", "fare_amount", true);
    table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
    table.head(5);
  }
  // {
  //   SDC::Dataframe table("NYCtaxi");
  //   table.filter("VendorID", "==", "3");
  //   table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
  //   table.head(5);
  // }

  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
  printf("Time measured: %.3f seconds.\n", elapsed.count() * 1e-9);
}

void optimize(){
  SDC::Dataframe table("NYCtaxi");
  table.optimize();
}

int main(int argc, char** argv) {

  reset_sdc();
  run_workload();
  optimize();
  run_workload();

  return 0;
}