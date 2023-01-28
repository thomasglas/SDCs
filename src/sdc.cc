#include "sdc.h"
#include "nlohmann/json.hpp"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <bitset>

namespace SDC{

void Dataframe::head(int rows){
    // load meta data block (with indexes/tables)
    load_metadata();

    // loads most suitable index for query
    json index = load_index();
    
    // load data blocks from most suitable index
    std::shared_ptr<arrow::Table> table = load_data(index);

    // apply filters
    std::shared_ptr<arrow::Array> filter_mask;
    arrow::Status st = compute_filter_mask(table, filter_mask);
    assert(st.ok()); 

    // apply projections
    st = apply_filters_projections(table, filter_mask);
    assert(st.ok()); 

    // print
    PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*(table->Slice(0,rows)), 4, &std::cout));

    // update metadata (& upload boolean mask)
    update_metadata();
}

dataType Dataframe::get_col_dataType(std::string column){
    dataType result;
    for(auto& col: metadata["columns"]){
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

arrow::Status Dataframe::apply_filters_projections(std::shared_ptr<arrow::Table>& table, std::shared_ptr<arrow::Array> boolean_mask){
    std::vector<std::shared_ptr<arrow::Array>> filtered_arrays;
    std::shared_ptr<arrow::Schema> schema = table->schema();
    std::vector<int> schema_drop_fields;

    auto columns = table->columns();
    for(int i=0; i<columns.size(); i++){
        if(std::find(projections.begin(),projections.end(),table->field(i)->name())!=projections.end()){
            arrow::Datum filtered_datum;
            ARROW_ASSIGN_OR_RAISE(filtered_datum,arrow::compute::CallFunction("array_filter", {columns[i]->chunk(0), boolean_mask})); 
            std::shared_ptr<arrow::Array> filtered_array = std::move(filtered_datum).make_array();
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

    table = arrow::Table::Make(schema, filtered_arrays);
    
    return arrow::Status::OK();
}

arrow::Status Dataframe::compute_filter_mask(std::shared_ptr<arrow::Table> table, std::shared_ptr<arrow::Array>& mask){
    
    std::vector<std::shared_ptr<arrow::Array>> boolean_masks;
    for(auto& filter: filters){
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
        required_columns.push_back(col);
        projections.push_back(col);
    }
}

void Dataframe::filter(std::string column, std::string operator_, std::string constant, bool is_col){
    required_columns.push_back(column);
    filters.push_back(Filter(column, operator_, constant, is_col, get_col_dataType(column)));
}

json Dataframe::load_index(bool get_primary){
    json indexes = metadata["indexes"];
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
    assert(table_name==index["table"]);

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

void Dataframe::load_metadata(){
    std::ifstream f("../data/metadata.json");
    json metadata_json = json::parse(f);
    for(auto& table: metadata_json["tables"]){
        if(table["name"]==table_name){
            metadata = table;
            return;
        }
    } 
    std::cout << "table " + table_name + " not found" << std::endl;
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
    for(auto& workload: metadata["workload"]){
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
        for(auto projection: projections){
            json metadata_projection;
            metadata_projection["name"] = projection;
            metadata_projections.push_back(metadata_projection);
        }

        json metadata_filters;
        for(auto filter: filters){
            json metadata_filter;
            metadata_filter["column"] = filter.column;
            metadata_filter["operator"] = filter.operator_;
            metadata_filter["constantOrColumn"] = filter.constant_or_column;
            metadata_filter["isCol"] = filter.is_col;
            metadata_filter["trueCount"] = filter.true_count;
            metadata_filter["falseCount"] = filter.false_count;
            // write arrow boolean array to disk
            std::string boolean_mask_file = "../data/boolean_filter_"+filter.column;
            write_boolean_filter(filter, boolean_mask_file); 
            metadata_filter["booleanMask"] = boolean_mask_file;
            metadata_filters.push_back(metadata_filter);
        }

        metadata_workload["filters"] = metadata_filters;
        metadata_workload["projections"] = metadata_projections;
            
        metadata["workload"].push_back(metadata_workload);
    }

    // read in file, replace table metadata
    std::ifstream f("../data/metadata.json");
    json metadata_json = json::parse(f);
    for(auto& table: metadata_json["tables"]){
        if(table["name"]==table_name){
            table = metadata;
            break;
        }
    } 

    // write out updated metadata
    std::ofstream o("../data/metadata.json");
    o << std::setw(2) << metadata_json << std::endl;
}

std::string Dataframe::get_query_id(){
    std::string query_id = table_name;
    for(auto filter: filters){
        query_id += filter.column + filter.operator_ + filter.constant_or_column;
    }
    for(auto projection: projections){
        query_id += projection;
    }
    return query_id;
}

std::shared_ptr<arrow::Array> Dataframe::read_boolean_filter(const std::string& filepath){
    std::ifstream in_file;
    std::shared_ptr<arrow::Array> boolean_filter;
    auto builder = arrow::BooleanBuilder();
    uint32_t num_valid_bits = metadata["num_rows"];
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

void Dataframe::optimize(){
    // get meta data
    load_metadata();

    // build qd tree
        // get all filters and projections
    std::vector<Filter> workload_filters;
    std::vector<std::string> workload_projections;
    for(const auto& metadata_workload: metadata["workload"]){
        for(const auto& metadata_filter: metadata_workload["filters"]){
            // load workload filters into Arrow arrays 
            Filter filter(metadata_filter["column"], metadata_filter["operator"], metadata_filter["constantOrColumn"], metadata_filter["isCol"], get_col_dataType(metadata_filter["column"]));
            filter.true_count = metadata_filter["trueCount"];
            filter.false_count = metadata_filter["falseCount"];
            filter.boolean_mask = read_boolean_filter(metadata_filter["booleanMask"]);
            workload_filters.push_back(filter);
        }
        for(const auto& metadata_projection: metadata_workload["projections"]){
            // load workload filters into Arrow arrays 
            workload_projections.push_back(metadata_projection["name"]);
        }
    }
        
    // build qd tree using:
        // columns used in workload -> go through all queries' projections & filters
        // boolean_masks for filters used -> 
    QDTree qd = QDTree(workload_filters, workload_projections, metadata);

    // get data from primary index
    json primary_index = load_index(true);
    std::shared_ptr<arrow::Table> data = load_data(primary_index);

    // write new data blocks, using qd tree filters on primary data
    // write qd tree index file
    // update metadata
}

}