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

int main(int argc, char** argv) {

  SDC::Dataframe table("NYCtaxi");
  // table.filter("VendorID", "<", "3");
  // table.filter("fare_amount", ">", "10");
  // table.filter("tip_amount", ">", "5");
  // table.filter("tip_amount", ">=", "fare_amount", true);
  // table.projection({"VendorID", "fare_amount", "tip_amount", "payment_type"});
  // table.head(10);
  table.optimize();

  return 0;
}