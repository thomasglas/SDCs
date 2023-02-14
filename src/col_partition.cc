#include "col_partition.h"

#include <iostream>
#include <queue>
#include <string>
#include <arrow/compute/api.h>
#include <parquet/exception.h>

namespace SDC{

ColPartition::ColPartition(std::string partition_column, std::vector<Filter>& filters, json& metadata)
:filters(filters)
{
    std::vector<Filter> column_filters;
    // group filters by column
    for(auto& filter: filters){
        if(filter.column==partition_column && !filter.is_col){ 
            column_filters.push_back(filter);
        }
    }

    assert(column_filters.size()>0);

    std::sort(column_filters.begin(), column_filters.end());
    // make cuts at each filter cut, save bool mask for each leaf node
    std::string previous_cut = "";
    bool previous_max_inclusive = false;
    std::shared_ptr<arrow::Array> remaining_tuples = nullptr;
    for(auto const& filter: column_filters){
        bool max_inclusive = false;
        std::shared_ptr<arrow::Array> partition_tuples;

        if((filter.operator_ == "<" || filter.operator_ == "<=") && filter.constant_or_column!=previous_cut){
            if(filter.operator_ == "<="){
                max_inclusive = true;
            }
            if(partitions.size()==0){
                partition_tuples = filter.boolean_mask;
            }
            else if(partitions.size()==1){
                // deduct tuples from previous partition from this partition
                auto st = arrow::compute::CallFunction("invert", {partitions[0].tuples});
                auto previous_tuples = st.ValueOrDie().make_array();
                st = arrow::compute::CallFunction("and", {previous_tuples, filter.boolean_mask});
                partition_tuples = st.ValueOrDie().make_array();
            }
            else{
                std::shared_ptr<arrow::Array> previous_tuples = partitions[0].tuples;
                for(size_t i=1; i<partitions.size(); i++){
                    auto st = arrow::compute::CallFunction("or", {previous_tuples, partitions[i].tuples});
                    previous_tuples = st.ValueOrDie().make_array();
                }
                auto st = arrow::compute::CallFunction("invert", {previous_tuples});
                auto inverted_previous_tuples = st.ValueOrDie().make_array();
                st = arrow::compute::CallFunction("and", {inverted_previous_tuples, filter.boolean_mask});
                partition_tuples = st.ValueOrDie().make_array();
            }
        }
        else if((filter.operator_ == ">" || filter.operator_ == ">=") && filter.constant_or_column!=previous_cut){
            if(filter.operator_ == ">"){
                max_inclusive = true;
            }
            if(partitions.size()==0){
                auto st = arrow::compute::CallFunction("invert", {filter.boolean_mask});
                auto inverted_filter = st.ValueOrDie().make_array();
                partition_tuples = inverted_filter;
            }
            else if(partitions.size()==1){
                // deduct tuples from previous partition from this partition
                auto st = arrow::compute::CallFunction("invert", {partitions[0].tuples});
                auto previous_tuples = st.ValueOrDie().make_array();
                st = arrow::compute::CallFunction("invert", {filter.boolean_mask});
                auto inverted_filter = st.ValueOrDie().make_array();
                st = arrow::compute::CallFunction("and", {previous_tuples, inverted_filter});
                partition_tuples = st.ValueOrDie().make_array();
            }
            else{
                std::shared_ptr<arrow::Array> previous_tuples = partitions[0].tuples;
                for(size_t i=1; i<partitions.size(); i++){
                    auto st = arrow::compute::CallFunction("or", {previous_tuples, partitions[i].tuples});
                    previous_tuples = st.ValueOrDie().make_array();
                }
                auto st = arrow::compute::CallFunction("invert", {previous_tuples});
                auto inverted_previous_tuples = st.ValueOrDie().make_array();
                st = arrow::compute::CallFunction("invert", {filter.boolean_mask});
                auto inverted_filter = st.ValueOrDie().make_array();
                st = arrow::compute::CallFunction("and", {inverted_previous_tuples, filter.boolean_mask});
                partition_tuples = st.ValueOrDie().make_array();
            }
        }
        else{
            continue;
        }
        auto part = Partition(partition_column, previous_cut, !max_inclusive, filter.constant_or_column, max_inclusive, filter.type);
        previous_cut = filter.constant_or_column;
        previous_max_inclusive = max_inclusive;

        auto st = arrow::compute::CallFunction("array_filter", {partition_tuples, partition_tuples});
        part.num_tuples = st.ValueOrDie().make_array()->length();
        part.tuples = partition_tuples;
        partitions.push_back(part);   
    }
    // final partition
    std::shared_ptr<arrow::Array> previous_tuples = partitions[0].tuples;
    for(size_t i=1; i<partitions.size(); i++){
        auto st = arrow::compute::CallFunction("or", {previous_tuples, partitions[i].tuples});
        previous_tuples = st.ValueOrDie().make_array();
    }
    auto st = arrow::compute::CallFunction("invert", {previous_tuples});
    auto leftover_tuples = st.ValueOrDie().make_array();
    auto part = Partition(partition_column, partitions[partitions.size()-1].max, !partitions[partitions.size()-1].max_inclusive, "", false, partitions[partitions.size()-1].col_data_type);

    st = arrow::compute::CallFunction("array_filter", {leftover_tuples, leftover_tuples});
    part.num_tuples = st.ValueOrDie().make_array()->length();
    part.tuples = leftover_tuples;
    partitions.push_back(part); 
};

}