#pragma once
#include<string>
#include"json.hpp"
using json=nlohmann::json;

inline std::string make_request(const std::string& method,int a,int b){
    json j;
    j["method"]=method;
    j["params"]={a,b};
    return j.dump();
}

inline int parse_response(const std::string& resp){
    json j=json::parse(resp);
    return j["result"];
}

inline std::string make_response(int result){
    json j;
    j["result"]=result;
    return j.dump();
}

