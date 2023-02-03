#ifndef INCLUDE_QD_TREE
#define INCLUDE_QD_TREE

#include <vector>
#include <string>
#include <memory>
#include <map>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include "filter.h"
#include "types.h"

namespace SDC{

class QDNodeRange {
    public:
        std::string column;
        std::string min;
        bool min_inclusive;
        std::string max;
        bool max_inclusive;
        dataType col_data_type;

        QDNodeRange(std::string column, std::string min, bool min_inclusive, std::string max, bool max_inclusive, dataType col_data_type)
        :column(column), min(min), max(max), col_data_type(col_data_type), min_inclusive(min_inclusive), max_inclusive(max_inclusive){};
};

class QDNode {
    public:
        enum nodeType{
            leafNode,
            innerNode,
            base
        } type;

        std::vector<QDNodeRange> ranges;
        std::shared_ptr<QDNode> parent_node;
        int num_tuples;
        bool is_true_child;
        std::shared_ptr<arrow::Array> tuples;

        std::string print(){
            switch(type){
                case nodeType::leafNode:{
                    return std::to_string(num_tuples);
                }
                case nodeType::innerNode:{
                    return std::to_string(num_tuples) + "(" + true_child->print() + "," + false_child->print() + ")";
                }
                default: return "error";
            }
        };
        
        // inner node
        Filter filter;
        std::shared_ptr<QDNode> true_child;
        std::shared_ptr<QDNode> false_child;

        QDNode()
        :type(nodeType::base),tuples(nullptr),parent_node(nullptr){};
};

class QDTree {
    public:
        QDTree(std::vector<Filter>& filters, std::vector<std::string>& projections, json& metadata);
        
        std::vector<std::shared_ptr<QDNode>> leafNodes;

        // columns used by workload
        std::vector<std::string> columns;

        // filters used by workload
        std::vector<Filter> filters;
    private:

        json metadata;

        // root node of QDTree
        std::shared_ptr<QDNode> root;

        size_t leaf_min_size = 100000;

        std::vector<QDNodeRange> add_range(std::vector<QDNodeRange> ranges, const Filter& filter, bool true_false_child);

        bool make_cut(std::shared_ptr<QDNode>& node, std::vector<SDC::Filter> &filters, json& workload);

        std::shared_ptr<arrow::Array> get_filter_mask(json& query_filter);
};

}

#endif // QD_TREE