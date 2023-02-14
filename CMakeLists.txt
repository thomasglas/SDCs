# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.0)

project(SDCs)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
# find_package(nlohmann_json 3.11.2 REQUIRED)

set(CMAKE_CXX_STANDARD 17)

# if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
# endif()

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER}")

file(GLOB SOURCES src/*.cc)

add_executable(sdcs ${SOURCES})

target_include_directories(sdcs PRIVATE include/)

if(ARROW_LINK_SHARED)
  target_link_libraries(sdcs PRIVATE Arrow::arrow_shared Parquet::parquet_shared ${AWSSDK_LINK_LIBRARIES})
else()
  target_link_libraries(sdcs PRIVATE Arrow::arrow_static Parquet::parquet_static ${AWSSDK_LINK_LIBRARIES})
endif()