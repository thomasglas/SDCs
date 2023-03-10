#ifndef INCLUDE_FILTER
#define INCLUDE_FILTER

#include <string>
#include <memory>
#include <arrow/api.h>

#include "types.h"

namespace SDC{

class Filter{
    public:
        Filter() = default;
        Filter(std::string col, std::string op, std::string const_, bool is_col, dataType type)
        :column(col), operator_(op), constant_or_column(const_), is_col(is_col), type(type)
        {}

        std::string column;
        std::string operator_;
        std::string constant_or_column;
        bool is_col;
        std::shared_ptr<arrow::Array> boolean_mask;
        dataType type;
        int true_count;
        int false_count;
        bool operator==(const Filter& rhs){
            return column==rhs.column && operator_==rhs.operator_ && type==rhs.type && is_col==rhs.is_col && constant_or_column==rhs.constant_or_column;
        }
        bool operator<(const Filter& rhs){
            return column==rhs.column && is_col==rhs.is_col && is_col==rhs.is_col && constant_or_column<rhs.constant_or_column;
        }
};

}

#endif