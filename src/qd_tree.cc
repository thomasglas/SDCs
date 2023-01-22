#include "qd_tree.h"

#include <arrow/compute/api.h>

namespace SDC{

QDTree::QDTree(std::vector<Filter>& filters, std::vector<std::string>& projections, json workload)
:columns(projections), filters(filters){
    // init: find first cut
    int max_tuples_cut = 0;
    int idx_argmax_tuples_cut;
    for(int i=0; i<filters.size(); i++){
        int tuples_cut = discarded_tuples(filters[i], workload);
        if(tuples_cut>max_tuples_cut){
            tuples_cut = max_tuples_cut;
            idx_argmax_tuples_cut = i;
        }
    }
    root = std::make_shared<QDInnerNode>(filters[idx_argmax_tuples_cut]);
}

uint64_t QDTree::discarded_tuples(Filter filter, json workload){

    auto st = arrow::compute::CallFunction("array_filter", {filter.boolean_mask, filter.boolean_mask});
    uint64_t count_true_tuples = st.ValueOrDie().make_array()->length();
    uint64_t count_false_tuples = filter.boolean_mask->length() - count_true_tuples;

    uint64_t tuples_discarded = 0;
    for(auto query: workload){
        for(auto query_filter: query["filters"]){
            if(query_filter["column"]==filter.column && query_filter["isCol"]==filter.is_col){
                // int
                int query_cut = std::stoi(std::string(query_filter["constantOrColumn"]));
                int filter_cut = std::stoi(filter.constant_or_column);
                if(filter.operator_=="<"){
                    if(query_filter["operator"]=="<" && query_cut<=filter_cut){
                        tuples_discarded += count_false_tuples;
                    }
                    else if(query_filter["operator"]=="<=" && query_cut<filter_cut){
                        tuples_discarded += count_false_tuples;
                    }
                    else if(query_filter["operator"]==">" && query_cut>=filter_cut-1){
                        tuples_discarded += count_true_tuples;
                    }
                    else if(query_filter["operator"]==">=" && query_cut>=filter_cut){
                        tuples_discarded += count_true_tuples;
                    }
                }
                else if(filter.operator_=="<="){
                    
                }
                else if(filter.operator_==">"){
                    
                }
                else if(filter.operator_==">="){
                    
                }
                else if(filter.operator_=="=="){
                    
                }
            }
        }
        // if query has a filter on same column which is equally or more restrictive, then can skip all false tuples
        // tuples_discarded += count(false_tuples) * query["executionCount"] 
    }
    return tuples_discarded;
}

}