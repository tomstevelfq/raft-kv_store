#pragma once
#include"json.hpp"
#include"ThreadPool.h"
#include<string>
#include<iostream>
#include<unordered_map>
#include<tuple>
#include<netinet/in.h>
#include<unistd.h>
#include<sys/select.h>
#include<type_traits>
#include<atomic>
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

    //普通函数重载
    template<typename R,typename... Args>
    void register_function(const std::string& name,R(*func)(Args...)){
        std::function<R(Args...)> fn=func;
        handlers[name]=[fn](const json& params)->json{
            R result=call_with_json(fn,params);
            return json{{"result",result}};
        };
    }

    //类成员函数重载
    template<typename C,typename R,typename... Args>
    void register_function(const std::string& name,C* obj,R(C::*mem)(Args...)){
        std::function<R(Args...)> fn=[obj,mem](Args... args)->R{
            return (obj->*mem)(std::forward<Args>(args)...);
        };

        handlers[name]=[fn](const json& params)->json{
            R result = call_with_json(fn,params);
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

    //启动一个rpc server
    void startRpcLoop(int port){
        int server_fd,new_socket;
        sockaddr_in address;
        memset(&address,0,sizeof(address));
        server_fd=socket(AF_INET,SOCK_STREAM,0);
        if(server_fd<0){
            perror("socket error");
            exit(1);
        }
        address.sin_family=AF_INET;
        address.sin_addr.s_addr=INADDR_ANY;
        address.sin_port=htons(port);
        int opt=1;
        setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
        int bin=bind(server_fd,(struct sockaddr*)&address,sizeof(address));
        std::cout<<bin<<std::endl;
        if(bin<0){
            std::cout<<"bind error"<<std::endl;
            exit(1);
        }
        listen(server_fd,3);
        std::cout<<"server listen on 9099 ..."<<std::endl;
        socklen_t addrlen=sizeof(address);

        fd_set readfds;
        std::vector<int> client_sockets;//存储客户端套接字

        //线程池
        ThreadPool pool(5);

        struct timeval timeout;
        timeout.tv_sec=3;
        timeout.tv_usec=0;

        while(!stop.load()){
            //清空文件描述符集合
            FD_ZERO(&readfds);
            FD_SET(server_fd,&readfds);
            int max_sd=server_fd;

            //添加所有客户端套接字到集合
            for(int client_socket:client_sockets){
                if(client_socket>0){
                    FD_SET(client_socket,&readfds);
                }

                if(client_socket>max_sd){
                    max_sd=client_socket;
                }
            }

            int activity=select(max_sd+1,&readfds,nullptr,nullptr,&timeout);
            if(activity==0){
                continue;
            }
            
            if(activity<0){
                std::cerr<<"select error!"<<std::endl;
                continue;
            }

            //检查服务器套接字新连接
            if(FD_ISSET(server_fd,&readfds)){
                new_socket=accept(server_fd,(struct sockaddr*)&address,&addrlen);
                if(new_socket<0){
                    perror("accept failed");
                    continue;
                }

                std::cout<<"new connection,socket fd: "<<new_socket<<std::endl;
                client_sockets.push_back(new_socket);//添加客户端套接字
            }

            //检查客户端套接字数据
            for(int i=0;i<client_sockets.size();i++){
                int client_socket=client_sockets[i];
                if(FD_ISSET(client_socket,&readfds)){
                    char buffer[1024]={0};
                    int bytes=read(client_socket,buffer,sizeof(buffer));
                    if(bytes<=0){
                        std::cout<<"closing connection,socket fd: "<<client_socket<<std::endl;
                        close(client_socket);
                        client_sockets.erase(client_sockets.begin()+i);
                        --i;
                    }else{
                        std::string req(buffer,bytes);
                        auto fn=[=]{
                            json j=json::parse(req);
                            auto ret=handle(j);
                            std::cout<<"response: "<<ret<<"method: "<<req<<std::endl;
                            std::string resp=ret.dump();
                            send(client_socket,resp.c_str(),resp.size(),0);
                        };
                        pool.submit(fn);
                    }
                }
            }
        }

        close(server_fd);
        for(auto sock_fd:client_sockets){
            close(sock_fd);
        }
    }

    void stopServer(){
        stop=true;
    }

    private:
    std::unordered_map<std::string,Handler> handlers;
    std::atomic<bool> stop=false;
};

