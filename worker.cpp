#include"master.hpp"
#include<iostream>
#include<unordered_map>
#include<thread>
#include<mutex>
#include<atomic>
#include<chrono>
#include<vector>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<unistd.h>
#include<cstring>

using namespace std::chrono;

class Worker{
public:
    Worker(std::string addr,int hbIntervalMs)
    :addr(addr),hbIntervalMs(hbIntervalMs),stop(false),id(0),R(0){}

    template<typename... Args>
    json request(const std::string& name,Args... args){
        char buffer[1024]={0};
        std::string req=make_request(name,args...);
        std::cout<<"req:"<<req<<std::endl;
        send(serverSock,req.c_str(),req.size(),0);
        int bytes=read(serverSock,buffer,1024);
        std::string resp(buffer,bytes);
        json j=json::parse(resp);
        return j;
    }

    void start(){
        serverSock=connectCoord("127.0.0.1",9099);
        json j=request("registerWorker","localhost");//向maser注册
        std::cout<<"result:"<<j["result"]<<std::endl;
        auto result=j["result"].get<std::pair<int,int>>();
        id=result.first;
        R=result.second;

        hbThread=std::thread(&Worker::heartbeatLoop,this);//启动心跳线程
        hbThread.join();
    }

    void setStatus(const std::string& s,int runningTask=-1){
        std::lock_guard<std::mutex> lk(mu);
        status=s;
        this->runningTask=runningTask;
    }

    //模拟崩溃
    void simulateCrash(){
        stop.store(true);
        if(hbThread.joinable()){
            hbThread.join();
        }
        std::cout<<"worker#"<<id<<" crashed (stop heartbeats)\n";
    }

    //正常关闭
    void shutdown(){
        stop.store(true);
        if(hbThread.joinable()){
            hbThread.join();
        }
        std::cout<<"worker#"<<id<<" shutdown\n";
    }


private:
    std::string addr;
    int hbIntervalMs;
    std::atomic<bool> stop;
    std::thread hbThread;//心跳线程
    int id;
    int R;
    std::mutex mu;
    std::string status="Idle";
    int runningTask=-1;
    int serverSock;

    void heartbeatLoop(){
        while(!stop.load()){
            {
                std::lock_guard<std::mutex> lk(mu);
                json j=request("heartbeat",id,status,runningTask);
                bool ok=j["result"];
                if(!ok){
                    std::cout<<"worker#"<<id<<" heartbeat rejected (not registered?)\n";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(hbIntervalMs));
        }
    }

    int connectCoord(const std::string& ip,const int port){
        int sock=0;
        sockaddr_in serv_addr;
        char buffer[1024]={0};
        sock=socket(AF_INET,SOCK_STREAM,0);
        serv_addr.sin_family=AF_INET;
        serv_addr.sin_port=htons(9099);

        inet_pton(AF_INET,ip.c_str(),&serv_addr.sin_addr);
        int conn=connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
        if(conn<0){
            return -1;
        }
        return sock;
    }
};

int main(){
    Worker worker("127.0.0.1",10000);
    worker.start();
    return 0;
}