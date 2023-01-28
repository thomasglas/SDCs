#ifndef INCLUDE_TYPES
#define INCLUDE_TYPES

#include <iostream>

namespace SDC{

enum dataType{
    int64,
    double_
};

std::string dataType_to_string(dataType d);

dataType string_to_dataType(std::string d);

}

#endif