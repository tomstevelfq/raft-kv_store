#pragma once
#include"json.hpp"
#include<string>
#include<iostream>
#include<unordered_map>
#include<tuple>
#include<type_traits>
using json=nlohmann::json;

template<typename T>
struct function_traits;

template<typename R,typename... Args>
struct function_traits<R(*)(Args...)>{
    using return_type=R;
    using args_tuple=std::tuple<Args...>;
    static constexpr size_t arity=sizeof...(Args);
};

template<typename R,typename... Args>
struct function_traits<std::function<R(Args...)>>{
    using return_type=R;
    using args_tuple=std::tuple<Args...>;
    static constexpr size_t arity=sizeof...(Args);
};

template<typename Func, typename Tuple, std::size_t... Is>
auto call_with_json_impl(Func f,const json& params,std::index_sequence<Is...>){
    return f(params[Is].get<std::tuple_element_t<Is,Tuple>>()...);
}

template<typename Func>
auto call_with_json(Func f,const json& params){
    using traits=function_traits<Func>;
    using args_tuple=typename traits::args_tuple;
    constexpr size_t N=traits::arity;
    return call_with_json_impl<Func,args_tuple>(f,params,std::make_index_sequence<N>{});
}

template<typename... Args>
inline std::string make_request(const std::string& method,Args&&... args){
    json j;
    j["method"]=method;
    j["params"]={std::forward<Args>(args)...};
    return j.dump();
}

class RPCServer{
    public:
    using Handler=std::function<json(const json&)>;

    template<typename R,typename... Args>
    void register_function(const std::string& name,R(*func)(Args...)){
        std::function<R(Args...)> fn=func;
        handlers[name]=[fn](const json& params)->json{
            R result=call_with_json(fn,params);
            return json{{"result",result}};
        };
    }

    json handle(const json& req){
        std::string method=req["method"];
        json params=req["params"];
        if(handlers.count(method)){
            return handlers[method](params);
        }else{
            return json{{"error","method not found"}};
        }
    }

    private:
    std::unordered_map<std::string,Handler> handlers;
};

