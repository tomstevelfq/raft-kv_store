遇到的问题：
按引用捕获方式去捕获指针，结果运行时出现段错误，类成员函数指针是无效值
template<typename C, typename R, typename... Args>
void register_function(const std::string& name, std::shared_ptr<C> obj, R(C::*mem)(Args...)) {
    std::function<R(Args...)> fn = [obj, &mem](Args... args) -> R {
        return (obj->*mem)(std::forward<Args>(args)...);
    };

    handlers[name] = [fn](const json& params) -> json {
        R result = call_with_json(fn, params);
        return json{{"result", result}};
    };
}

//mem应该使用值捕获，不然超出函数作用域会失效
template<typename C, typename R, typename... Args>
void register_function(const std::string& name, std::shared_ptr<C> obj, R(C::*mem)(Args...)) {
    std::function<R(Args...)> fn = [obj, mem](Args... args) -> R {
        return (obj->*mem)(std::forward<Args>(args)...);
    };

    handlers[name] = [fn](const json& params) -> json {
        R result = call_with_json(fn, params);
        return json{{"result", result}};
    };
}
