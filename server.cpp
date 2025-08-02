#include"rpc.hpp"
#include"ThreadPool.h"
#include<iostream>
#include<netinet/in.h>
#include<unistd.h>
#include<cstring>
#include<thread>

int add(int a,int b){
    return a+b;
}

std::string greeting(){
    return "hello world";
}

std::string repeat(int num,std::string str){
    std::string newstr;
    for(int i=0;i<num;i++){
        newstr+=str;
    }
    return newstr;
}

int main(){
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
    address.sin_port=htons(9099);
    int opt=1;
    setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
    int bin=bind(server_fd,(struct sockaddr*)&address,sizeof(address));
    std::cout<<bin<<std::endl;
    if(bin<0){
        std::cout<<"bind error"<<std::endl;
        exit(1);
    }
    int l=listen(server_fd,3);
    std::cout<<"server listen on 9099 ..."<<std::endl;
    socklen_t addrlen=sizeof(address);

    //线程池
    ThreadPool pool(5);

    //rpc注册
    RPCServer rpcServer;
    rpcServer.register_function("add",add);
    rpcServer.register_function("greeting",greeting);
    rpcServer.register_function("repeat",repeat);

    while(true){
        new_socket=accept(server_fd,(struct sockaddr*)&address,&addrlen);
        auto task = [=,&rpcServer]{
            char buffer[1024]={0};
            int bytes=read(new_socket,buffer,1024);
            std::string req(buffer,bytes);
            json j=json::parse(req);

            auto ret=rpcServer.handle(j);

            std::cout<<ret<<std::endl;

            std::string resp=ret.dump();
            std::cout<<resp<<std::endl;
            send(new_socket,resp.c_str(),resp.size(),0);
            close(new_socket);
        };
        pool.submit(task);
    }
    close(server_fd);
    return 0;
}