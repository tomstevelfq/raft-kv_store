#include<iostream>
#include"common.h"
#include"rpc.hpp"

int main(){
    int sock=0;
    sockaddr_in serv_addr;
    char buffer[1024]={0};
    sock=socket(AF_INET,SOCK_STREAM,0);
    serv_addr.sin_family=AF_INET;
    serv_addr.sin_port=htons(9099);

    inet_pton(AF_INET,"127.0.0.1",&serv_addr.sin_addr);
    int conn=connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
    if(conn<0){
        return -1;
    }
    std::string req=make_request("testfunc");
    std::cout<<"req:"<<req<<std::endl;
    send(sock,req.c_str(),req.size(),0);
    int bytes=read(sock,buffer,1024);
    if(bytes<=0){
        json j;
        j["result"]=-1;
        return j;
    }
    std::string resp(buffer,bytes);
    std::cout<<resp<<std::endl;
    return 0;
}