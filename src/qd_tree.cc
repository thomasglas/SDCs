#include "qd_tree.h"

#include <iostream>
#include <queue>
#include <string>
#include <arrow/compute/api.h>
#include <parquet/exception.h>

namespace SDC{

QDTree::QDTree(std::vector<Filter>& filters, std::vector<std::string>& projections, json& metadata)
:columns(projections), filters(filters), metadata(metadata){
    assert(filters.size()>0);
    // init root: find first cut
    root = std::make_shared<QDNode>();
    root->num_tuples = metadata["num_rows"];

    std::queue<std::shared_ptr<QDNode>> nodeQueue;
    nodeQueue.push(root);
    while(!nodeQueue.empty()){
        auto base_node = std::static_pointer_cast<QDNode>(nodeQueue.front());

        if(make_cut(base_node, filters, metadata["workload"])){
            nodeQueue.push(base_node->true_child);
            nodeQueue.push(base_node->false_child);
        }
        else{
            // turn node into leaf node
            leafNodes.push_back(base_node);
        }
        nodeQueue.pop();
    }
    
    std::cout << root->print() << std::endl;

    // build data blocks for leaf nodes
    // apply ranges as filters on data
    // write resulting data to parquet files
    // create index metadata, add path to main metadata
    // add data blocks to index metadata
}

std::shared_ptr<arrow::Array> QDTree::get_filter_mask(json& query_filter){
    for(auto& filter: filters){
        if(filter.column==query_filter["column"] && filter.operator_==query_filter["operator"] 
            && filter.is_col==query_filter["isCol"] && filter.constant_or_column==query_filter["constantOrColumn"])
        {
            return filter.boolean_mask;
        }
    }
    // should not reach this
    assert(1==2);
}

bool QDTree::make_cut(std::shared_ptr<QDNode>& node, std::vector<SDC::Filter> &filters, json& workload){
    if(node->num_tuples<2*leaf_min_size){
        return false;
    }

    // given a filter:
        // loop through all workload queries, find query filters on same columm, compute discarded tuples
        // sum up total discarded tuples over all workload queries
        // discarded tuples = #true(tuples) - #true(tuples X child_filter)
    
    int max_tuples_discarded = 0;
    int idx_argmax_tuples_cut = -1;
    std::shared_ptr<arrow::Array> argmax_true_filtered_node_tuples;
    std::shared_ptr<arrow::Array> argmax_false_filtered_node_tuples;
    int argmax_count_tuples_true;
    int argmax_count_tuples_false;
    for(int i=0; i<filters.size(); i++){
        std::shared_ptr<arrow::Array> true_filtered_node_tuples;
        std::shared_ptr<arrow::Array> false_filtered_node_tuples;
        int count_tuples_true;
        int count_tuples_false;
        if(node->tuples==nullptr){
            count_tuples_true = filters[i].true_count;
            count_tuples_false = filters[i].false_count;

            true_filtered_node_tuples = filters[i].boolean_mask;
            auto st = arrow::compute::CallFunction("invert", {filters[i].boolean_mask});
            false_filtered_node_tuples = st.ValueOrDie().make_array();
        }
        else{
            // compute filter between tuples in node and filter
            auto st = arrow::compute::CallFunction("and", {node->tuples, filters[i].boolean_mask});
            true_filtered_node_tuples = st.ValueOrDie().make_array();
            
            // how many tuples left after filter
            st = arrow::compute::CallFunction("array_filter", {true_filtered_node_tuples, true_filtered_node_tuples});
            count_tuples_true = st.ValueOrDie().make_array()->length();
            count_tuples_false = node->num_tuples - count_tuples_true;

            
            st = arrow::compute::CallFunction("invert", {filters[i].boolean_mask});
            auto inverted_filter = st.ValueOrDie().make_array();
            st = arrow::compute::CallFunction("and", {node->tuples, inverted_filter});
            false_filtered_node_tuples = st.ValueOrDie().make_array();
        }

        // minimum size for data blocks
        if(count_tuples_true<leaf_min_size || count_tuples_false<leaf_min_size){
            continue;
        }

        int tuples_discarded = 0;
        for(auto query: workload){
            for(auto query_filter: query["filters"]){
                if(query_filter["column"]==filters[i].column && query_filter["isCol"]==filters[i].is_col){

                    // discarded tuples = #true(tuples) - #true(tuples X filter)
                    switch(filters[i].type){
                        case dataType::int64:{
                            int query_cut = std::stoi(std::string(query_filter["constantOrColumn"]));
                            int filter_cut = std::stoi(filters[i].constant_or_column);
                            if(filters[i].operator_=="<"){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut-1){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                            }
                            else if(filters[i].operator_=="<="){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut+1){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                            }
                            else if(filters[i].operator_==">"){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut+1){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                            }
                            else if(filters[i].operator_==">="){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut-1){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                            }
                            else if(filters[i].operator_=="=="){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut-1){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="!=" && query_cut==filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut!=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                            }
                            break;
                        }
                        case dataType::double_:{
                            double query_cut = std::stod(std::string(query_filter["constantOrColumn"]));
                            double filter_cut = std::stod(filters[i].constant_or_column);
                            if(filters[i].operator_=="<"){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                            }
                            else if(filters[i].operator_=="<="){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                            }
                            else if(filters[i].operator_==">"){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                            }
                            else if(filters[i].operator_==">="){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_false;
                                }
                                else if(query_filter["operator"]=="==" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                            }
                            else if(filters[i].operator_=="=="){
                                if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="<=" && query_cut<filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">" && query_cut>=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]==">=" && query_cut>filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="!=" && query_cut==filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                                else if(query_filter["operator"]=="==" && query_cut!=filter_cut){
                                    tuples_discarded += count_tuples_true;
                                }
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }
        if(tuples_discarded>max_tuples_discarded){
            max_tuples_discarded = tuples_discarded;
            idx_argmax_tuples_cut = i;
            argmax_true_filtered_node_tuples = true_filtered_node_tuples;
            argmax_count_tuples_true = count_tuples_true;
            argmax_count_tuples_false = count_tuples_false;
            argmax_false_filtered_node_tuples = false_filtered_node_tuples;
        }
    }
    if(idx_argmax_tuples_cut>-1){
        // change node type to inner node
        node->type = QDNode::nodeType::innerNode;
        node->filter = filters[idx_argmax_tuples_cut];

        // init true child
        auto true_child = std::make_shared<QDNode>();
        true_child->parent_node = node;
        true_child->ranges = add_range(node->ranges, node->filter, true);
        true_child->num_tuples = argmax_count_tuples_true;
        true_child->is_true_child = true;
        true_child->tuples = argmax_true_filtered_node_tuples;
        node->true_child = true_child;

        // init false child
        auto false_child = std::make_shared<QDNode>();
        false_child->parent_node = node;
        false_child->ranges = add_range(node->ranges, node->filter, false);
        false_child->num_tuples = argmax_count_tuples_false;
        false_child->is_true_child = false;
        false_child->tuples = argmax_false_filtered_node_tuples;
        node->false_child = false_child;

        return true;
    }
    else{
        // turn node into leaf node
        node->filePath = "test.parquet";
        node->type = QDNode::nodeType::leafNode;
        return false;
    }
}

std::vector<QDNodeRange> QDTree::add_range(std::vector<QDNodeRange> ranges, const Filter& filter, bool is_true_child){
    bool found_range;
    for(auto& range: ranges){
        if(range.column==filter.column){
            switch(range.col_data_type){
                case dataType::int64:{
                    if(filter.operator_=="<" && range.max!="" && !filter.is_col && std::stoi(filter.constant_or_column)<std::stoi(range.max)){
                        if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = false;
                        }
                        else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = true;
                        }
                    }
                    else if(filter.operator_=="=<" && range.max!="" && !filter.is_col && std::stoi(filter.constant_or_column)<std::stoi(range.max)){
                         if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = true;
                         }
                         else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = false;
                         }
                    }
                    else if(filter.operator_==">" && range.min!= "" && !filter.is_col && std::stoi(filter.constant_or_column)>std::stoi(range.min)){
                        if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = false;
                        }
                        else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = true;
                        }
                    }
                    else if(filter.operator_==">=" && range.min!= "" && !filter.is_col && std::stoi(filter.constant_or_column)>std::stoi(range.min)){
                        if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = true;
                        }
                        else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = false;
                        }
                    }
                    break;
                }
                case dataType::double_:{
                    if(filter.operator_=="<" && range.max!="" && !filter.is_col && std::stod(filter.constant_or_column)<std::stod(range.max)){
                        if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = false;
                        }
                        else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = true;
                        }
                    }
                    else if(filter.operator_=="=<" && range.max!="" && !filter.is_col && std::stod(filter.constant_or_column)<std::stod(range.max)){
                        if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = true;
                        }
                        else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = false;
                        }
                    }
                    else if(filter.operator_==">" && range.min!= "" && !filter.is_col && std::stod(filter.constant_or_column)>std::stod(range.min)){
                        if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = false;
                        }
                        else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = true;
                        }
                    }
                    else if(filter.operator_==">=" && range.min!= "" && !filter.is_col && std::stod(filter.constant_or_column)>std::stod(range.min)){
                        if(is_true_child){
                            range.max = filter.constant_or_column;
                            range.max_inclusive = true;
                        }
                        else{
                            range.min = filter.constant_or_column;
                            range.min_inclusive = false;
                        }
                    }
                    break;
                }
            }
            found_range = true;
            break;
        }
    }
    if(!found_range){
        if(filter.operator_=="<"){
            if(is_true_child){
                ranges.push_back(QDNodeRange(filter.column, "", true, filter.constant_or_column, false, filter.type));
            }
            else{
                ranges.push_back(QDNodeRange(filter.column, filter.constant_or_column, true, "", true, filter.type));
            }
        }
        else if(filter.operator_=="<="){
            if(is_true_child){
                ranges.push_back(QDNodeRange(filter.column, "", true, filter.constant_or_column, true, filter.type));
            }
            else{
                ranges.push_back(QDNodeRange(filter.column, filter.constant_or_column, false, "", true, filter.type));
            }
        }
        else if(filter.operator_==">"){
            if(is_true_child){
                ranges.push_back(QDNodeRange(filter.column, filter.constant_or_column, false, "", true, filter.type));
            }
            else{
                ranges.push_back(QDNodeRange(filter.column, "", true, filter.constant_or_column, true, filter.type));
            }
        }
        else if(filter.operator_==">="){
            if(is_true_child){
                ranges.push_back(QDNodeRange(filter.column, filter.constant_or_column, true, "", true, filter.type));
            }
            else{
                ranges.push_back(QDNodeRange(filter.column, "", true, filter.constant_or_column, false, filter.type));
            }
        }
    }
    return ranges;
}

}