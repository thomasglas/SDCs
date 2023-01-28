#include "sdc.h"
#include "nlohmann/json.hpp"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <bitset>
#include <filesystem>
#include <algorithm>

namespace SDC{

void Dataframe::head(int rows){
    // load meta data block (with indexes/tables)
    _metadata = load_metadata();

    // loads most suitable index for query
    json index = load_index();
    
    // load data blocks from most suitable index
    std::shared_ptr<arrow::Table> table = load_data(index);

    // apply filters
    std::shared_ptr<arrow::Array> filter_mask;
    arrow::Status st = compute_filter_mask(table, filter_mask);
    assert(st.ok()); 

    // apply projections
    std::shared_ptr<arrow::Table> filtered_table = apply_filters_projections(table, _projections, filter_mask);

    // print
    PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*(filtered_table->Slice(0,rows)), 4, &std::cout));

    // update metadata (& upload boolean mask)
    update_metadata();
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

std::shared_ptr<arrow::Table> Dataframe::apply_filters_projections(const std::shared_ptr<arrow::Table>& table, const std::vector<std::string>& projections, std::shared_ptr<arrow::Array> boolean_mask){
    std::vector<std::shared_ptr<arrow::Array>> filtered_arrays;
    std::shared_ptr<arrow::Schema> schema = table->schema();
    std::vector<int> schema_drop_fields;

    auto columns = table->columns();
    for(int i=0; i<columns.size(); i++){
        if(std::find(projections.begin(),projections.end(),table->field(i)->name())!=projections.end()){
            auto st = arrow::compute::CallFunction("array_filter", {columns[i]->chunk(0), boolean_mask});
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

    return arrow::Table::Make(schema, filtered_arrays);
}

arrow::Status Dataframe::compute_filter_mask(std::shared_ptr<arrow::Table> table, std::shared_ptr<arrow::Array>& mask){
    
    std::vector<std::shared_ptr<arrow::Array>> boolean_masks;
    for(auto& filter: _filters){
        std::shared_ptr<arrow::Array> column_array = table->GetColumnByName(filter.column)->chunk(0); 
        arrow::Datum boolean_mask_datum;
        if(filter.is_col){
            std::shared_ptr<arrow::Array> compare_column_array = table->GetColumnByName(filter.constant_or_column)->chunk(0); 
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
        boolean_masks.push_back(filter.boolean_mask);

        auto st = arrow::compute::CallFunction("array_filter", {filter.boolean_mask, filter.boolean_mask});
        filter.true_count = st.ValueOrDie().make_array()->length();
        filter.false_count = filter.boolean_mask->length() - filter.true_count;
    }

    // combine masks
    mask = boolean_masks[0];
    arrow::Datum boolean_mask_datum;
    for(size_t i=1; i<boolean_masks.size(); i++){
        ARROW_ASSIGN_OR_RAISE(boolean_mask_datum,arrow::compute::CallFunction("and", {mask, boolean_masks[i]}));
        mask = std::move(boolean_mask_datum).make_array();
    }

    return arrow::Status::OK();
}

std::string Dataframe::get_arrow_compute_operator(std::string filter_operator){
    std::string result;
    if(filter_operator=="="){
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

json Dataframe::load_index(bool get_primary){
    json indexes = _metadata["indexes"];
    if(indexes.size()==1 || get_primary){
        for(auto index: indexes){
            if(index["type"]=="primary"){
                std::ifstream f(index["filePath"]);
                return json::parse(f);
            }
        }       
        // should not reach this
        assert(1==2);
    }
    else{
        // find most suitable index (has columns, built on filters)
        for(auto& idx: indexes){
            // todo
        }
        std::ifstream f(indexes[0]["filePath"]);
        return json::parse(f);
    }
}

std::shared_ptr<arrow::Table> Dataframe::load_data(json index){
    assert(_table_name==index["table"]);

    // load all tables
    std::vector<std::shared_ptr<arrow::Table>> data_blocks;
    for(auto& block: index["dataBlocks"]){
        data_blocks.push_back(load_parquet(block["filePath"]));
    }

    // merge tables
    arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::ConcatenateTables(data_blocks);
    if(!result.ok()){
        std::cout << result.status().ToString() << std::endl;
    }
    return result.ValueOrDie();
}

std::shared_ptr<arrow::Table> Dataframe::load_parquet(std::string file_path){
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile,arrow::io::ReadableFile::Open(file_path,arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns() << " columns." << std::endl;
    for(auto field: table->schema()->fields()){
    std::cout << field->name() << ":" << field->type()->name() << std::endl;
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
    std::cout << "table " + _table_name + " not found" << std::endl;
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
            // write arrow boolean array to disk
            std::string boolean_mask_file = data_directory+"/boolean_filter_"+filter.column;
            write_boolean_filter(filter, boolean_mask_file); 
            metadata_filter["booleanMask"] = boolean_mask_file;
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
  std::cout << "Writing " << table->num_rows() << " rows and " << table->num_columns() << " columns." << std::endl;
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(file_path));
  // The last argument to the function call is the size of the RowGroup in the parquet file
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*(table.get()), arrow::default_memory_pool(), outfile, 1000000));

  std::cout << "Done writing." << std::endl;
  return arrow::Status::OK();
}

void Dataframe::optimize(){
    // get table meta data
    _metadata = load_metadata();

    // build qd tree
        // get all filters and projections
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
        
    // build qd tree using:
        // columns used in workload -> go through all queries' projections & filters
        // boolean_masks for filters used -> 
    QDTree qd = QDTree(workload_filters, workload_projections, _metadata);

    // get data from primary index
    json primary_index = load_index(true);
    std::shared_ptr<arrow::Table> table = load_data(primary_index);

    // remove previous qd tree index
    for(int i=0; i<_metadata["indexes"].size(); i++){
        if(_metadata["indexes"][i]["type"]=="qdTree"){
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

    std::filesystem::create_directories(data_directory+"/qd_index");

    json qd_index;
    qd_index["table"] = _table_name;
    qd_index["indexType"] = "qdTree";
    qd_index["dataBlocks"] = {};

    // write new data blocks, using qd tree filters on primary data
    int block_ids = 0;
    for(auto leafNode: qd.leafNodes){
        std::string file_path = data_directory+"/qd_index/data_block_"+std::to_string(block_ids++);
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
        
        dataBlock["filePath"] = file_path;
        std::shared_ptr<arrow::Table> filtered_table = apply_filters_projections(table, qd.columns, leafNode->tuples);
        assert(write_parquet_file(filtered_table, file_path).ok());
        qd_index["dataBlocks"].push_back(dataBlock);
    }
    
    // create qd index metadata
    std::ofstream o(data_directory+"/qd_index.json");
    o << std::setw(2) << qd_index << std::endl;

    // update metadata
    json metadata_indexes_qd;
    metadata_indexes_qd["filePath"] = data_directory+"/qd_index.json";
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
    std::ofstream o2("../data/metadata.json");
    o2 << std::setw(2) << metadata_json << std::endl;
}

}