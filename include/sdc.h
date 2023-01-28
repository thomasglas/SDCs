#ifndef INCLUDE_SDC
#define INCLUDE_SDC

#include <vector>
#include <string>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <arrow/scalar.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <fstream>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include "qd_tree.h"
#include "filter.h"
#include "types.h"

namespace SDC{

class Dataframe {
    public:
        Dataframe(std::string table)
        : _table_name(table)
        {
            data_directory = "../data/"+_table_name;
        };
        void head(int rows=0);
        void filter(std::string column, std::string operator_, std::string constant, bool is_col=false);
        void projection(std::vector<std::string> projections);
        void optimize();

    private:
        std::string data_directory;
        std::string _table_name;
        std::vector<Filter> _filters;
        std::vector<std::string> _projections;
        std::vector<std::string> _required_columns;
        json _metadata;
        void update_metadata();
        json load_metadata();
        json load_index(bool get_primary=false);
        std::string get_query_id();
        void write_boolean_filter(Filter& filter, const std::string& filepath);
        std::shared_ptr<arrow::Array> read_boolean_filter(const std::string& filepath);
        std::shared_ptr<arrow::Table> load_data(json index);

        // arrow & parquet
        std::shared_ptr<arrow::Table> load_parquet(std::string file_path);
        arrow::Status compute_filter_mask(std::shared_ptr<arrow::Table> table, std::shared_ptr<arrow::Array>& mask);
        dataType get_col_dataType(std::string column);
        std::string get_arrow_compute_operator(std::string filter_operator);
        std::shared_ptr<arrow::Table> apply_filters_projections(const std::shared_ptr<arrow::Table>& table, const std::vector<std::string>& projections, std::shared_ptr<arrow::Array> boolean_mask);
        arrow::Status write_parquet_file(const std::shared_ptr<arrow::Table>& table, const std::string& file_path);
};

} // namespace SDC

#endif // SDC