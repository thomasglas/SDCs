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
#include "col_partition.h"
#include "filter.h"
#include "types.h"

namespace SDC{

class Dataframe {
    public:
        Dataframe(std::string table, bool add_latency=false, bool verbose=false)
        : _table_name(table), _add_latency(add_latency), _verbose(verbose), _data_directory("../data/"+table){};
        void head(int use_index=1, int rows=0);
        void filter(std::string column, std::string operator_, std::string constant, bool is_col=false);
        void projection(std::vector<std::string> projections);
        void group_by(std::string function_name);
        void optimize(std::string partition_column, int min_leaf_size);

    private:
        std::string _data_directory;
        std::string _table_name;
        std::string _group_by;
        std::vector<Filter> _filters;
        std::vector<std::string> _projections;
        std::vector<std::string> _required_columns;
        json _metadata;
        bool _using_primary_index;
        bool _verbose;
        bool _add_latency;
        void update_metadata();
        json load_metadata();
        json load_index(int use_index);
        std::string get_query_id();
        void write_boolean_filter(Filter& filter, const std::string& filepath);
        std::shared_ptr<arrow::Array> read_boolean_filter(const std::string& filepath);
        std::shared_ptr<arrow::Table> load_data(json index);
        void remove_index(std::string index_type);
        json qdTree_metadata_file(QDTree qd, std::shared_ptr<arrow::Table> table);
        json metadata_qdTree_index(QDTree qd);
        json colPartition_metadata_file(ColPartition cp, std::shared_ptr<arrow::Table> table);
        json metadata_columnPartition_index(ColPartition cp);
        void add_latency(std::string path);
        void apply_group_by(const std::shared_ptr<arrow::Table>& table);

        // arrow & parquet
        std::shared_ptr<arrow::Table> load_parquet(std::string file_path);
        arrow::Status compute_filter_mask(std::shared_ptr<arrow::Table> table, std::vector<std::shared_ptr<arrow::Array>>& masks);
        dataType get_col_dataType(std::string column);
        std::string get_arrow_compute_operator(std::string filter_operator);
        std::shared_ptr<arrow::Table> apply_filters_projections(const std::shared_ptr<arrow::Table>& table, const std::vector<std::string>& projections, std::vector<std::shared_ptr<arrow::Array>> boolean_masks);
        arrow::Status write_parquet_file(const std::shared_ptr<arrow::Table>& table, const std::string& file_path);
};

} // namespace SDC

#endif // SDC