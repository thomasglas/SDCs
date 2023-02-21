#include "sdc.h"
#include "nlohmann/json.hpp"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <bitset>
#include <filesystem>
#include <string>
#include <algorithm>
#include <chrono>
#include <thread>
#include <filesystem>

namespace SDC{

void Dataframe::head(int use_index, int rows){
    // load meta data block (with indexes/tables)
    _metadata = load_metadata();

    // loads most suitable index for query
    json index = load_index(use_index);
    
    // load data blocks from most suitable index
    std::shared_ptr<arrow::Table> table = load_data(index);
    
    if(_verbose){
        std::cout << "number of loaded rows: " << table->num_rows() << std::endl;
    }

    // apply filters
    std::vector<std::shared_ptr<arrow::Array>> filter_mask;
    arrow::Status st = compute_filter_mask(table, filter_mask);
    assert(st.ok()); 

    // apply projections
    std::shared_ptr<arrow::Table> filtered_table = apply_filters_projections(table, _projections, filter_mask);

    // apply group by


    // print
    if(_verbose){
        std::cout << "number of filtered_rows: " << filtered_table->num_rows() << std::endl;
        PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*(filtered_table->Slice(0,rows)), 4, &std::cout));
    }

    // update metadata (& upload boolean mask, if primary key was used)
    update_metadata();
}

void Dataframe::add_latency(std::string path){
    // std::cout << "get file: " << path << std::endl;
    if(_add_latency){
        // latency
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        // throughput
        // 10GB/s = 10000B/s
        uintmax_t sleep = std::filesystem::file_size(path)/10000;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep));
    }
    return;
}

dataType Dataframe::get_col_dataType(std::string column){
    dataType result;
    for(auto& col: _metadata["columns"]){
        if(col["name"]==column){
            if(col["dataType"]=="int64"){
                result = dataType::int64;
            }
            else if(col["dataType"]=="double"){
                result = dataType::double_;
            }
            break;
        }
    }
    return result;
}

void Dataframe::apply_group_by(const std::shared_ptr<arrow::Table>& table){
    // // count options
    // arrow::compute::CountOptions options;
    // options.mode = arrow::compute::CountOptions::ONLY_VALID;
    
    // // scalar options
    // arrow::compute::ScalarAggregateOptions scalar_aggregate_options;
    // scalar_aggregate_options.skip_nulls = false;

    // arrow::compute::HashAggregateFunction("hash_count", 
    //     arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc(), &options);

    // arrow::compute::
}

void Dataframe::group_by(std::string function_name){
    _group_by = function_name;
}

std::shared_ptr<arrow::Table> Dataframe::apply_filters_projections(const std::shared_ptr<arrow::Table>& table, const std::vector<std::string>& projections, std::vector<std::shared_ptr<arrow::Array>> boolean_masks){
    
    std::vector<std::shared_ptr<arrow::Table>> table_chunks;
    for(size_t j=0; j<boolean_masks.size(); j++){
        std::vector<std::shared_ptr<arrow::Array>> filtered_arrays;
        std::shared_ptr<arrow::Schema> schema = table->schema();
        std::vector<int> schema_drop_fields;

        auto columns = table->columns();
        for(int i=0; i<columns.size(); i++){
            if(std::find(projections.begin(),projections.end(),table->field(i)->name())!=projections.end() || projections.empty()){
                assert(columns[i]->num_chunks()==boolean_masks.size());
                auto a1 = columns[i];
                auto a = columns[i]->chunk(j);
                auto b = boolean_masks[j];
                auto st = arrow::compute::CallFunction("array_filter", {columns[i]->chunk(j), boolean_masks[j]});
                std::shared_ptr<arrow::Array> filtered_array = st.ValueOrDie().make_array();
                filtered_arrays.push_back(filtered_array);
            }
            else{
                schema_drop_fields.push_back(i);
            }
        }

        for(int i=schema_drop_fields.size()-1; i>=0; i--){
            auto result = schema->RemoveField(schema_drop_fields[i]);
            if(!result.ok()){
                std::cout << result.status().ToString() << std::endl;
            }
            schema = result.ValueOrDie();
        }

        table_chunks.push_back(arrow::Table::Make(schema, filtered_arrays));
    }

    // merge tables
    arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::ConcatenateTables(table_chunks);
    if(!result.ok()){
        std::cout << result.status().ToString() << std::endl;
    }
    return result.ValueOrDie();
}

arrow::Status Dataframe::compute_filter_mask(std::shared_ptr<arrow::Table> table, std::vector<std::shared_ptr<arrow::Array>>& masks){

    std::vector<std::vector<std::shared_ptr<arrow::Array>>> chunks_filters_boolean_masks; // for each chunk, all filter masks 
    for(size_t i=0; i<table->column(0)->num_chunks(); i++){
        std::vector<std::shared_ptr<arrow::Array>> filters_boolean_masks;
        for(auto& filter: _filters){
            std::shared_ptr<arrow::Array> column_array = table->GetColumnByName(filter.column)->chunk(i); 
            arrow::Datum boolean_mask_datum;
            if(filter.is_col){
                std::shared_ptr<arrow::Array> compare_column_array = table->GetColumnByName(filter.constant_or_column)->chunk(i); 
                ARROW_ASSIGN_OR_RAISE(boolean_mask_datum,arrow::compute::CallFunction(get_arrow_compute_operator(filter.operator_), {column_array, compare_column_array}));
            }
            else{
                std::shared_ptr<arrow::Scalar> constant;
                switch(get_col_dataType(filter.column)){
                    case dataType::int64:{
                        constant = arrow::MakeScalar<int64_t>(std::stoi(filter.constant_or_column));
                        break;
                    }
                    case dataType::double_:{
                        constant = arrow::MakeScalar<double>(std::stod(filter.constant_or_column));
                        break;
                    }
                }
                ARROW_ASSIGN_OR_RAISE(boolean_mask_datum,arrow::compute::CallFunction(get_arrow_compute_operator(filter.operator_), {column_array, constant}));
            }
            filter.boolean_mask = std::move(boolean_mask_datum).make_array();
            filters_boolean_masks.push_back(filter.boolean_mask);

            // filter metadata will only be written to sdc metadata, if currently using primary index
            auto st = arrow::compute::CallFunction("array_filter", {filter.boolean_mask, filter.boolean_mask});
            filter.true_count = st.ValueOrDie().make_array()->length();
            filter.false_count = filter.boolean_mask->length() - filter.true_count;
        }
        chunks_filters_boolean_masks.push_back(filters_boolean_masks);
    }

    // resize to num_chunks
    masks.resize(chunks_filters_boolean_masks.size());

    arrow::Datum boolean_mask_datum;
    // loop through chunks
    for(size_t i=0; i<chunks_filters_boolean_masks.size(); i++){
        masks[i] = chunks_filters_boolean_masks[i][0];
        // loop through mask for each filter, combine filters
        for(size_t j=1; j<chunks_filters_boolean_masks[i].size(); j++){
        // for(const auto& filter_boolean_mask: chunks_filters_boolean_masks[i]){
            ARROW_ASSIGN_OR_RAISE(boolean_mask_datum,arrow::compute::CallFunction("and", {masks[i], chunks_filters_boolean_masks[i][j]}));
            masks[i] = std::move(boolean_mask_datum).make_array();
        }
    }

    return arrow::Status::OK();
}

std::string Dataframe::get_arrow_compute_operator(std::string filter_operator){
    std::string result;
    if(filter_operator=="=="){
        result = "equal";
    }
    else if(filter_operator=="<"){
        result = "less";
    }
    else if(filter_operator==">"){
        result = "greater";
    }
    else if(filter_operator=="<="){
        result = "less_equal";
    }
    else if(filter_operator==">="){
        result = "greater_equal";
    }
    else if(filter_operator=="!="){
        result = "not_equal";
    }

    return result;
}

void Dataframe::projection(std::vector<std::string> columns){
    for(auto& col: columns){
        _required_columns.push_back(col);
        _projections.push_back(col);
    }
}

void Dataframe::filter(std::string column, std::string operator_, std::string constant, bool is_col){
    _required_columns.push_back(column);
    _filters.push_back(Filter(column, operator_, constant, is_col, get_col_dataType(column)));
}

json Dataframe::load_index(int use_index){
    json indexes = _metadata["indexes"];
    if(indexes.size()==1 || use_index==1){
        _using_primary_index = true;
        for(auto index: indexes){
            if(index["type"]=="primary"){
                std::ifstream f(index["filePath"]);
                return json::parse(f);
            }
        }       
        // should not reach this
        assert(1==2);
    }
    else if(use_index==2){
        // column partition index
        json columnPartition;
        for(auto& index: indexes){
            if(index["type"]=="columnPartition"){
                columnPartition = index;
            }
        }

        _using_primary_index = false;
        std::ifstream f(columnPartition["filePath"]);
        return json::parse(f);
    }
    else{
        assert(use_index==3);
        // qd_tree index
        json qd_tree;
        for(auto& index: indexes){
            if(index["type"]=="qdTree"){
                qd_tree = index;
            }
        }

        // check if qdTree has all required columns
        bool has_all_columns = true;
        for(const auto& proj: _projections){
            bool found = false;
            for(const auto& col: qd_tree["columns"]){
                if(proj==col["name"]){
                    found = true;
                    break;
                }
            }
            if(!found){
                has_all_columns = false;
                break;
            }
        }
        _using_primary_index = false;
        std::ifstream f(qd_tree["filePath"]);
        return json::parse(f);
    }
}

std::shared_ptr<arrow::Table> Dataframe::load_data(json index){
    assert(_table_name==index["table"]);

    // load all tables
    std::vector<std::shared_ptr<arrow::Table>> data_blocks;
    for(auto& block: index["dataBlocks"]){
        // check: does data block contain data which the query needs?
        bool is_relevant = true;
        for(const auto& range: block["ranges"]){
            // find filters on same column
            for(const auto& filter: _filters){
                if(range["column"]==filter.column && !filter.is_col){
                    // check if data block and filter have overlap
                    switch(string_to_dataType(range["colDataType"])){

                        case dataType::int64:{
                            if(filter.operator_=="<" && range["min"]!=""
                                && (std::stoi(range["min"].get<std::string>()) >= std::stoi(filter.constant_or_column)
                                || (std::stoi(range["min"].get<std::string>()) > std::stoi(filter.constant_or_column)-1 && !range["minInclusive"]))){
                                is_relevant = false;

                            }
                            else if(filter.operator_=="<=" && range["min"]!="" 
                                && (std::stoi(range["min"].get<std::string>()) > std::stoi(filter.constant_or_column)
                                || (std::stoi(range["min"].get<std::string>()) >= std::stoi(filter.constant_or_column) && !range["minInclusive"]))){
                                is_relevant = false;
                            }
                            else if(filter.operator_==">" && range["max"]!="" 
                                && (std::stoi(range["max"].get<std::string>()) <= std::stoi(filter.constant_or_column)
                                || (std::stoi(range["max"].get<std::string>()) < std::stoi(filter.constant_or_column)+1 && !range["maxInclusive"]))){
                                is_relevant = false;
                            }
                            else if(filter.operator_==">=" && range["max"]!="" 
                                && (std::stoi(range["max"].get<std::string>()) < std::stoi(filter.constant_or_column)
                                || (std::stoi(range["max"].get<std::string>()) <= std::stoi(filter.constant_or_column) && !range["maxInclusive"]))){
                                is_relevant = false;
                            }
                            else if(filter.operator_=="==" && 
                                ((range["max"]!="" && std::stoi(range["max"].get<std::string>()) < std::stoi(filter.constant_or_column))
                                || (range["max"]!="" && std::stoi(range["max"].get<std::string>()) <= std::stoi(filter.constant_or_column) && !range["maxInclusive"])
                                || (range["min"]!="" && std::stoi(range["min"].get<std::string>()) > std::stoi(filter.constant_or_column))
                                || (range["min"]!="" && std::stoi(range["min"].get<std::string>()) >= std::stoi(filter.constant_or_column) && !range["minInclusive"]))){
                                is_relevant = false;
                            }
                            break;
                        }
                        case dataType::double_:{
                            if(filter.operator_=="<" && range["min"]!="" 
                                && std::stod(range["min"].get<std::string>()) >= std::stod(filter.constant_or_column)){
                                is_relevant = false;
                            }
                            else if(filter.operator_=="<=" && range["min"]!="" 
                                && (std::stod(range["min"].get<std::string>()) > std::stod(filter.constant_or_column)
                                || (std::stod(range["min"].get<std::string>()) >= std::stod(filter.constant_or_column) && !range["minInclusive"]))){
                                is_relevant = false;
                            }
                            else if(filter.operator_==">" && range["max"]!="" && std::stod(range["max"].get<std::string>()) <= std::stod(filter.constant_or_column)){
                                is_relevant = false;
                            }
                            else if(filter.operator_==">=" && range["max"]!="" 
                                && (std::stod(range["max"].get<std::string>()) < std::stod(filter.constant_or_column)
                                || (std::stod(range["max"].get<std::string>()) <= std::stod(filter.constant_or_column) && !range["maxInclusive"]))){
                                is_relevant = false;
                            }
                            else if(filter.operator_=="==" 
                                && ((range["max"]!="" && std::stod(range["max"].get<std::string>()) < std::stod(filter.constant_or_column)) 
                                || (range["max"]!="" && std::stod(range["max"].get<std::string>()) <= std::stod(filter.constant_or_column) && !range["maxInclusive"]) 
                                || (range["min"]!="" && std::stod(range["min"].get<std::string>()) > std::stod(filter.constant_or_column))
                                || (range["min"]!="" && std::stod(range["min"].get<std::string>()) >= std::stod(filter.constant_or_column) && !range["minInclusive"]))){
                                is_relevant = false;
                            }
                            break;
                        }
                        default:{
                            // should not reach this
                            assert(1==2);
                        }
                    }
                }
                if(!is_relevant){
                    break;
                }
            }
            if(!is_relevant){
                break;
            }
        }
        if(is_relevant){
            data_blocks.push_back(load_parquet(block["filePath"]));
        }
    }

    // merge tables
    arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::ConcatenateTables(data_blocks);
    if(!result.ok()){
        std::cout << result.status().ToString() << std::endl;
    }
    return result.ValueOrDie();
}

std::shared_ptr<arrow::Table> Dataframe::load_parquet(std::string file_path){
    add_latency(file_path);
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile,arrow::io::ReadableFile::Open(file_path,arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    if(_verbose){
        std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns() << " columns." << std::endl;
        for(auto field: table->schema()->fields()){
            std::cout << field->name() << ":" << field->type()->name() << std::endl;
        }
    }

    return table;
}

json Dataframe::load_metadata(){
    std::ifstream f("../data/metadata.json");
    json metadata_json = json::parse(f);
    for(auto& table: metadata_json["tables"]){
        if(table["name"]==_table_name){
            return table;
        }
    } 
    if(_verbose){
        std::cout << "table " + _table_name + " not found" << std::endl;
    }
    return json();
}

void Dataframe::write_boolean_filter(Filter& filter, const std::string& filepath){
    
    std::ofstream out_file;
    // trunc will clear the file
    out_file.open(filepath, out_file.binary | out_file.trunc);
    if(!out_file.is_open()){
        std::cerr << "Failed to open " << filepath << std::endl;
    }
    else{
        auto array = std::static_pointer_cast<arrow::BooleanArray>(filter.boolean_mask);
        size_t mask_length = filter.boolean_mask->length();
        size_t num_bytes = mask_length/8 + (mask_length%8 != 0);
        for(int i=0; i<num_bytes; i++){
            // ch: 00000000
            char ch = 0;
            for(int j=0; j<8; j++){
                int k = i * 8 + j;
                if(k<mask_length){
                    ch |= (array->Value(k) << (8-j-1));
                    // std::cout << std::bitset<8>(ch) << std::endl;
                }
            }
            out_file.write(&ch, sizeof(ch));
        }
    }
}

// add each used filter to workload, add boolean masks for each filter
void Dataframe::update_metadata(){

    // update metadata
    std::string query_id = get_query_id();
    bool query_found = false;
    for(auto& workload: _metadata["workload"]){
        if(workload["queryID"]==query_id){
            int executionCount = workload["executionCount"];
            workload["executionCount"] = ++executionCount;
            query_found = true;
            break;
        }
    }
    if(!query_found){
        json metadata_workload;
        metadata_workload["queryID"] = query_id;
        metadata_workload["executionCount"] = 1;

        json metadata_projections;
        for(auto projection: _projections){
            json metadata_projection;
            metadata_projection["name"] = projection;
            metadata_projections.push_back(metadata_projection);
        }

        json metadata_filters;
        for(auto filter: _filters){
            json metadata_filter;
            metadata_filter["column"] = filter.column;
            metadata_filter["operator"] = filter.operator_;
            metadata_filter["constantOrColumn"] = filter.constant_or_column;
            metadata_filter["isCol"] = filter.is_col;
            metadata_filter["trueCount"] = filter.true_count;
            metadata_filter["falseCount"] = filter.false_count;
            if(_using_primary_index){
                // write arrow boolean array to disk
                std::string boolean_mask_file = _data_directory+"/boolean_filter_"+filter.column+filter.operator_+filter.constant_or_column;
                write_boolean_filter(filter, boolean_mask_file); 
                metadata_filter["booleanMask"] = boolean_mask_file;
            }
            metadata_filters.push_back(metadata_filter);
        }

        metadata_workload["filters"] = metadata_filters;
        metadata_workload["projections"] = metadata_projections;
            
        _metadata["workload"].push_back(metadata_workload);
    }

    // read in file, replace table metadata
    std::ifstream f("../data/metadata.json");
    json metadata_json = json::parse(f);
    for(auto& table: metadata_json["tables"]){
        if(table["name"]==_table_name){
            table = _metadata;
            break;
        }
    } 

    // write out updated metadata
    std::ofstream o("../data/metadata.json");
    o << std::setw(2) << metadata_json << std::endl;
}

std::string Dataframe::get_query_id(){
    std::string query_id = _table_name;
    for(auto filter: _filters){
        query_id += filter.column + filter.operator_ + filter.constant_or_column;
    }
    for(auto projection: _projections){
        query_id += projection;
    }
    return query_id;
}

std::shared_ptr<arrow::Array> Dataframe::read_boolean_filter(const std::string& filepath){
    std::ifstream in_file;
    std::shared_ptr<arrow::Array> boolean_filter;
    auto builder = arrow::BooleanBuilder();
    int num_valid_bits = _metadata["num_rows"];
    in_file.open(filepath, in_file.binary);
    if(!in_file.is_open()){
        std::cerr << "Failed to open " << filepath << std::endl;
    }
    else{
        int bit_counter = 0;
        while(in_file.peek()!=EOF){
            char ch;
            in_file.read(&ch, sizeof(ch));
            for (int i=0; i<8; i++){
                if(bit_counter<num_valid_bits){
                    bool bit = (ch >> (8-1-i)) & 1U;
                    arrow::Status a = builder.Append(bit);
                    bit_counter++;
                }
            }
        }
    }
    auto st = builder.Finish(&boolean_filter);
    assert(st.ok());

    return boolean_filter;
}

// Write out the data as a Parquet file
arrow::Status Dataframe::write_parquet_file(const std::shared_ptr<arrow::Table>& table, const std::string& file_path) {
    if(_verbose){
        std::cout << "Writing " << table->num_rows() << " rows and " << table->num_columns() << " columns." << std::endl;
    }
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(file_path));
    // The last argument to the function call is the size of the RowGroup in the parquet file
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*(table.get()), arrow::default_memory_pool(), outfile, 1000000));
    return arrow::Status::OK();
}

void Dataframe::remove_index(std::string index_type){
    // remove previous qd tree index
    for(int i=0; i<_metadata["indexes"].size(); i++){
        if(_metadata["indexes"][i]["type"]==index_type){
            // remove data blocks
            std::ifstream f(_metadata["indexes"][i]["filePath"]);
            json index = json::parse(f);
            for(auto dataBlock: index["dataBlocks"]){
                std::filesystem::remove(std::string(dataBlock["filePath"]));
            }
            // remove index file
            std::filesystem::remove(std::string(_metadata["indexes"][i]["filePath"]));
            // remove qd index
            _metadata["indexes"].erase(i);
            break;
        }
    }
    std::filesystem::create_directories(_data_directory+"/"+index_type);
}

json Dataframe::qdTree_metadata_file(QDTree qd, std::shared_ptr<arrow::Table> table){
    json qd_index;
    qd_index["table"] = _table_name;
    qd_index["indexType"] = "qdTree";
    qd_index["dataBlocks"] = {};

    // write new data blocks, using qd tree filters on primary data
    int block_ids = 0;
    for(auto leafNode: qd.leafNodes){
        std::string file_path = _data_directory+"/qdTree/data_block_"+std::to_string(block_ids++)+".parquet";
        json dataBlock;

        // add ranges
        for(auto range: leafNode->ranges){
            json json_range;
            json_range["column"] = range.column;
            json_range["min"] = range.min;
            json_range["minInclusive"] = range.min_inclusive;
            json_range["max"] = range.max;
            json_range["maxInclusive"] = range.max_inclusive;
            json_range["colDataType"] = dataType_to_string(range.col_data_type);
            dataBlock["ranges"].push_back(json_range);
        }
        
        dataBlock["numRows"] = leafNode->num_tuples;
        dataBlock["filePath"] = file_path;
        std::shared_ptr<arrow::Table> filtered_table = apply_filters_projections(table, qd.columns, {leafNode->tuples});
        assert(write_parquet_file(filtered_table, file_path).ok());
        qd_index["dataBlocks"].push_back(dataBlock);
    }
    return qd_index;
}

json Dataframe::colPartition_metadata_file(ColPartition cp, std::shared_ptr<arrow::Table> table){
    json cp_index;
    cp_index["table"] = _table_name;
    cp_index["indexType"] = "columnPartition";
    cp_index["dataBlocks"] = {};

    // write new data blocks, using cp partitions on primary data
    int block_ids = 0;
    for(auto partition: cp.partitions){
        std::string file_path = _data_directory+"/column_partition/data_block_"+std::to_string(block_ids++)+".parquet";
        json dataBlock;

        // add ranges
        json json_range;
        json_range["column"] = partition.column;
        json_range["min"] = partition.min;
        json_range["minInclusive"] = partition.min_inclusive;
        json_range["max"] = partition.max;
        json_range["maxInclusive"] = partition.max_inclusive;
        json_range["colDataType"] = dataType_to_string(partition.col_data_type);
        dataBlock["ranges"].push_back(json_range);
        
        dataBlock["filePath"] = file_path;
        std::shared_ptr<arrow::Table> filtered_table = apply_filters_projections(table, {}, {partition.tuples});
        assert(write_parquet_file(filtered_table, file_path).ok());
        dataBlock["numRows"] = partition.num_tuples;
        cp_index["dataBlocks"].push_back(dataBlock);
    }
    return cp_index;
}

json Dataframe::metadata_qdTree_index(QDTree qd){
    json metadata_indexes_qd;
    metadata_indexes_qd["filePath"] = _data_directory+"/qd_index.json";
    metadata_indexes_qd["type"] = "qdTree";
    metadata_indexes_qd["columns"] = {};
    for(auto column: qd.columns){
        json col;
        col["name"] = column;
        metadata_indexes_qd["columns"].push_back(col);
    }
    metadata_indexes_qd["filtersUsed"] = {};
    for(auto filter: qd.filters){
        json fil;
        fil["column"] = filter.column;
        fil["operator"] = filter.operator_;
        fil["constant"] = filter.constant_or_column;
        metadata_indexes_qd["filtersUsed"].push_back(fil);
    }
    return metadata_indexes_qd;
}

json Dataframe::metadata_columnPartition_index(ColPartition cp){
    json metadata_indexes_cp;
    metadata_indexes_cp["filePath"] = _data_directory+"/cp_index.json";
    metadata_indexes_cp["type"] = "columnPartition";
    metadata_indexes_cp["filtersUsed"] = {};
    for(auto filter: cp.filters){
        json fil;
        fil["column"] = filter.column;
        fil["operator"] = filter.operator_;
        fil["constant"] = filter.constant_or_column;
        metadata_indexes_cp["filtersUsed"].push_back(fil);
    }
    return metadata_indexes_cp;
}

void Dataframe::optimize(std::string partition_column, int min_leaf_size){
    // get table meta data
    _metadata = load_metadata();

    // get all filters and projections for qd tree
    std::vector<Filter> workload_filters;
    std::vector<std::string> workload_projections;
    for(const auto& metadata_workload: _metadata["workload"]){
        for(const auto& metadata_filter: metadata_workload["filters"]){
            // load workload filters into Arrow arrays 
            Filter filter(metadata_filter["column"], metadata_filter["operator"], metadata_filter["constantOrColumn"], metadata_filter["isCol"], get_col_dataType(metadata_filter["column"]));
            if(std::find(workload_filters.begin(), workload_filters.end(), filter)==workload_filters.end()){
                filter.true_count = metadata_filter["trueCount"];
                filter.false_count = metadata_filter["falseCount"];
                filter.boolean_mask = read_boolean_filter(metadata_filter["booleanMask"]);
                workload_filters.push_back(filter);
            }
        }
        for(const auto& metadata_projection: metadata_workload["projections"]){
            // load workload filters into Arrow arrays 
            if(std::find(workload_projections.begin(), workload_projections.end(), metadata_projection["name"])==workload_projections.end()){
                workload_projections.push_back(metadata_projection["name"]);
            }
        }
    }

    // get table from primary index
    json primary_index = load_index(true);
    std::shared_ptr<arrow::Table> table = load_data(primary_index);
 
    // ---------- COLUMN PARTITION ---------- //
    ColPartition colP = ColPartition(partition_column, workload_filters, _metadata);

    remove_index("column_partition");

    json colP_index_file = colPartition_metadata_file(colP, table);
    
    // write qd index metadata file
    std::ofstream o(_data_directory+"/cp_index.json");
    o << std::setw(2) << colP_index_file << std::endl;

    // update metadata
    json metadata_indexes_cp = metadata_columnPartition_index(colP);
    _metadata["indexes"].push_back(metadata_indexes_cp);

    
    // ---------- QD TREE ---------- //
    QDTree qd = QDTree(workload_filters, workload_projections, _metadata, min_leaf_size);

    if(_verbose){
        std::cout << qd.root->print() << std::endl;
    }

    // make sure all tuples are included
    std::shared_ptr<arrow::Array> tuples_included = qd.leafNodes[0]->tuples;
    for(size_t i=1; i<qd.leafNodes.size(); i++){
        auto st = arrow::compute::CallFunction("or", {tuples_included, qd.leafNodes[i]->tuples});
        tuples_included = st.ValueOrDie().make_array();
    }
    auto st = arrow::compute::CallFunction("invert", {tuples_included});
    std::shared_ptr<arrow::Array> tuples_not_included = st.ValueOrDie().make_array();
    st = arrow::compute::CallFunction("array_filter", {tuples_not_included, tuples_not_included});
    assert(st.ValueOrDie().make_array()->length()==0);

    remove_index("qdTree");
    json qd_index = qdTree_metadata_file(qd, table);
    
    // write qd index metadata file
    std::ofstream o2(_data_directory+"/qd_index.json");
    o2 << std::setw(2) << qd_index << std::endl;

    // update metadata
    json metadata_indexes_qd = metadata_qdTree_index(qd);
    _metadata["indexes"].push_back(metadata_indexes_qd);

    // read in file, replace table metadata
    std::ifstream f("../data/metadata.json");
    json metadata_json = json::parse(f);
    for(auto& table: metadata_json["tables"]){
        if(table["name"]==_table_name){
            table = _metadata;
            break;
        }
    } 

    // write out updated metadata
    std::ofstream o3("../data/metadata.json");
    o3 << std::setw(2) << metadata_json << std::endl;
}

}