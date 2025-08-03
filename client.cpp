#include"rpc.hpp"
#include<iostream>
#include<arpa/inet.h>
#include<unistd.h>
#include<cstring>

int main(){
    for(int i=0;i<10;i++){
        int sock=0;
        sockaddr_in serv_addr;
        char buffer[1024]={0};
        sock=socket(AF_INET,SOCK_STREAM,0);
        serv_addr.sin_family=AF_INET;
        serv_addr.sin_port=htons(9099);

        inet_pton(AF_INET,"127.0.0.1",&serv_addr.sin_addr);
        int conn=connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
        std::cout<<conn<<std::endl;
        std::string req=make_request("hello","tom");
        std::cout<<req<<std::endl;
        send(sock,req.c_str(),req.size(),0);
        int bytes=read(sock,buffer,1024);
        std::string resp(buffer,bytes);
        //int result = parse_response(resp);
        std::cout<<"RPC Call result:"<<resp<<std::endl;
        close(sock);
    }
    return 0;
}