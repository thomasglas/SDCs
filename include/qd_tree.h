#ifndef INCLUDE_QD_TREE
#define INCLUDE_QD_TREE

#include <vector>
#include <string>
#include <memory>
#include <map>

namespace SDC{

class QDNode {
    enum nodeType{
        leafNode,
        innerNode
    } type;
};

class QDLeafNode: public QDNode {
    std::string file_path;
    std::map<std::string,std::string> columns_ranges; // map columns to range contained
};

class QDInnerNode: public QDNode {
    std::string filter;
    int true_count;
    int false_count;
    std::shared_ptr<QDNode> true_child;
    std::shared_ptr<QDNode> false_child;
};

class QDTree {
    public:
        QDTree(std::vector<std::string> projections, std::vector<std::string> selections_counts){
            /*
                while(!selections.empty()){
                    next_selection = argmax(selection_true_counts);
                    
                }
            */
        };
    private:
        std::string table;

        // columns contained in data blocks
        std::vector<std::string> columns;

        // root node of QDTree
        std::shared_ptr<QDNode> root;
};

}

#endif // QD_TREE