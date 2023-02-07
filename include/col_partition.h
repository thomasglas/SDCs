#ifndef INCLUDE_COL_PARTITION
#define INCLUDE_COL_PARTITION

#include <vector>
#include <string>
#include <memory>
#include <map>
#include <algorithm>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include "filter.h"
#include "types.h"

namespace SDC{

class Partition {
    public:
        std::string column;
        std::string min;
        bool min_inclusive;
        std::string max;
        bool max_inclusive;
        dataType col_data_type;
        int num_tuples;
        std::shared_ptr<arrow::Array> tuples;

        Partition(std::string column, std::string min, bool min_inclusive, std::string max, bool max_inclusive, dataType col_data_type)
        :column(column), min(min), max(max), col_data_type(col_data_type), min_inclusive(min_inclusive), max_inclusive(max_inclusive){};
};

class ColPartition {
    public:
        ColPartition(std::string partition_column, std::vector<Filter>& filters, std::vector<std::string>& projections, json& metadata);
        std::vector<Partition> partitions;
        std::vector<std::string> projections;
        std::vector<Filter> filters;
};

}

#endif