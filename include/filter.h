#ifndef INCLUDE_FILTER
#define INCLUDE_FILTER

#include <string>
#include <memory>
#include <arrow/api.h>

namespace SDC{

class Filter{
    public:
        Filter(std::string col, std::string op, std::string const_, bool is_col)
        :column(col), operator_(op), constant_or_column(const_), is_col(is_col)
        {}
        std::string column;
        std::string operator_;
        std::string constant_or_column;
        bool is_col;
        std::shared_ptr<arrow::Array> boolean_mask;
};

}

#endif