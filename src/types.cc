#include "types.h"

namespace SDC{

std::string dataType_to_string(dataType d){
    switch(d){
        case dataType::int64:{
            return "int";
        }
        case dataType::double_:{
            return "double";
        }
        default: return "";
    }
}

dataType string_to_dataType(std::string d){
    if(d=="int"){
        return dataType::int64;
    }
    else if(d=="double"){
        return dataType::double_;
    }
    else{
        std::cout << "string to datatype error: " + d << std::endl;
        return dataType::int64;
    }
}

}