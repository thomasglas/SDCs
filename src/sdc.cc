#include "sdc.h"
#include "nlohmann/json.hpp"
#include <fstream>
#include <iomanip>
#include <iostream>

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
};

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
};

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
};

arrow::Status Dataframe::compute_filter_mask(std::shared_ptr<arrow::Table> table, std::shared_ptr<arrow::Array>& mask){
    
    std::vector<std::shared_ptr<arrow::Array>> boolean_masks;
    for(auto& filter: filters){
        std::shared_ptr<arrow::Array> column_array = table->GetColumnByName(filter.column)->chunk(0); 
        if(filter.is_col){
            std::shared_ptr<arrow::Array> compare_column_array = table->GetColumnByName(filter.constant_or_column)->chunk(0); 
            arrow::Datum boolean_mask_datum;
            ARROW_ASSIGN_OR_RAISE(boolean_mask_datum,arrow::compute::CallFunction(get_arrow_compute_operator(filter.operator_), {column_array, compare_column_array}));
            boolean_masks.push_back(std::move(boolean_mask_datum).make_array());
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
            arrow::Datum boolean_mask_datum;
            ARROW_ASSIGN_OR_RAISE(boolean_mask_datum,arrow::compute::CallFunction(get_arrow_compute_operator(filter.operator_), {column_array, constant}));
            boolean_masks.push_back(std::move(boolean_mask_datum).make_array());
        }
    }

    // combine masks
    mask = boolean_masks[0];
    arrow::Datum boolean_mask_datum;
    for(size_t i=1; i<boolean_masks.size(); i++){
        ARROW_ASSIGN_OR_RAISE(boolean_mask_datum,arrow::compute::CallFunction("and", {mask, boolean_masks[i]}));
        mask = std::move(boolean_mask_datum).make_array();
    }

    return arrow::Status::OK();
};

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
};

void Dataframe::projection(std::vector<std::string> columns){
    for(auto& col: columns){
        required_columns.push_back(col);
        projections.push_back(col);
    }
};

void Dataframe::filter(std::string column, std::string operator_, std::string constant, bool is_col){
    required_columns.push_back(column);
    filters.push_back(Filter(column, operator_, constant, is_col));
};

json Dataframe::load_index(){
    json indexes = metadata["indexes"];
    if(indexes.size()==1){
        std::ifstream f(indexes[0]["filePath"]);
        return json::parse(f);
    }
    else{
        // find most suitable index (has columns, built on filters)
        for(auto& idx: indexes){
            // todo
        }
        std::ifstream f(indexes[0]["filePath"]);
        return json::parse(f);
    }
};

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
};

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
};

void Dataframe::update_metadata(){
    // // read a JSON file
    // std::ifstream i("file.json");
    // json j;
    // i >> j;

    // // write prettified JSON to another file
    // std::ofstream o("pretty.json");
    // o << std::setw(4) << j << std::endl;
};

}