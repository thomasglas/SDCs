#ifndef INCLUDE_QD_TREE
#define INCLUDE_QD_TREE

#include <vector>
#include <string>
#include <memory>
#include <map>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include "filter.h"

namespace SDC{

class QDNode {
    public:
        enum nodeType{
            leafNode,
            innerNode
        } type;
        
    QDNode(nodeType type)
    :type(type){};
};

class QDLeafNode: public QDNode {
    public:
        QDLeafNode()
        :QDNode(nodeType::leafNode){};
        std::string file_path;
        std::map<std::string,std::string> columns_ranges; // map columns to range contained
};

class QDInnerNode: public QDNode {
    public:
        QDInnerNode(Filter filter)
        :QDNode(nodeType::innerNode),filter(filter){};
        Filter filter;
        std::shared_ptr<QDNode> true_child;
        std::shared_ptr<QDNode> false_child;
};

class QDTree {
    public:
        QDTree(std::vector<Filter>& filters, std::vector<std::string>& projections, json workload);
    private:
        std::string table;

        // columns contained in data blocks
        std::vector<std::string> columns;

        // filters used by data block
        std::vector<Filter> filters;

        // root node of QDTree
        std::shared_ptr<QDNode> root;

        uint64_t discarded_tuples(Filter filter, json workload);
};

}

#endif // QD_TREE