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

#include "sdc.h"

// Write out the data as a Parquet file
arrow::Status write_parquet_file(const arrow::Table& table) {
  std::cout << "Writing " << table.num_rows() << " rows and " << table.num_columns() << " columns." << std::endl;
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open("filtered_data.parquet"));
  // The last argument to the function call is the size of the RowGroup in
  // the parquet file. Normally you would choose this to be rather large but
  // for the example, we use a small value to have multiple RowGroups.
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 1000000));

  std::cout << "Done writing." << std::endl;
  return arrow::Status::OK();
}

int main(int argc, char** argv) {

  SDC::Dataframe table("NYCtaxi");
  // table.filter("VendorID", "<", "2");
  // table.filter("fare_amount", ">", "10");
  // table.filter("tip_amount", ">=", "fare_amount", true);
  // table.projection({"VendorID", "fare_amount", "tip_amount"});
  // table.head(10);
  table.optimize();

  return 0;
}