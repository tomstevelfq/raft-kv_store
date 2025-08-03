#pragma once
#include"rpc.hpp"
#include<iostream>
#include<map>
#include<vector>
#include<functional>
#include<mutex>
#include<thread>
#include<unordered_map>
#include<chrono>
#include<csignal>

using namespace std::chrono;

using MapKV=std::pair<std::string,std::string>;
using MapFn=std::function<std::vector<MapKV>(const std::string&,const std::string&)>;
using ReduceFn=std::function<std::string(const std::string&,const std::vector<std::string>&)>;

struct WorkerInfo{
    int id=0;
    std::string addr;
    steady_clock::time_point lastHB;//最近一次心跳时间
    std::string status="Idle";
    int runningTask=-1;
    bool alive=true;  //是否存活
};

class Coordinator{
    private:
    int nReduce;
    std::vector<std::pair<std::string,std::string>> inputs;
    MapFn mapfn;
    ReduceFn reducefn;
    int R;
    int timeoutMs;
    std::atomic<int> nextId;
    std::atomic<bool> stop;
    std::unordered_map<int,WorkerInfo> workers;
    std::mutex mu;
    RPCServer rpcServer;
    std::thread rpcLoop,mLoop;
    const int PORT=9099;

    public:
    explicit Coordinator(int R,int timeoutMs):R(R),timeoutMs(timeoutMs),nextId(1),stop(false){}

    std::pair<int,int> registerWorker(std::string addr){
        std::lock_guard<std::mutex> lk(mu);
        int id=nextId++;
        WorkerInfo wi;
        wi.id=id;
        wi.addr=addr;
        wi.lastHB=steady_clock::now();
        wi.status="Register";
        wi.alive=true;
        workers[id]=wi;
        std::cout<<"Coordinator worker#"<<id<<" registered from "<<addr<<"(R=)"<<R<<std::endl;
        return {id,R};
    }

    //心跳
    bool heartbeat(int workerId,const std::string status,int runningTask){
        std::lock_guard<std::mutex> lk(mu);
        auto it=workers.find(workerId);
        if(it==workers.end()){
            return false;
        }
        it->second.lastHB=steady_clock::now();
        it->second.status=status;
        it->second.runningTask=runningTask;
        if(!it->second.alive){
            it->second.alive=true;
            std::cout<<"coordinator worker#"<<workerId<<" revived by heartbeat"<<std::endl;
        }
        return true;
    }

    void rpcRegister(){
        rpcServer.register_function("registerWorker",this,&Coordinator::registerWorker);
        rpcServer.register_function("heartbeat",this,&Coordinator::heartbeat);
    }

    void handleLoop(){
        rpcRegister();//注册rpc
        rpcLoop=std::thread([this]{this->rpcServer.startRpcLoop(PORT);});//开启循环
        mLoop=std::thread([this]{this->monitorLoop();});
        handle_sigint();
    }

    void monitorLoop(){
        while(!stop.load()){
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            std::vector<int> justDead;
            {
                std::lock_guard<std::mutex> lk(mu);
                auto now=steady_clock::now();
                for(auto& [id,w]: workers){
                    auto ms=duration_cast<milliseconds>(now-w.lastHB).count();
                    if(w.alive&&ms>timeoutMs){
                        w.alive=false;
                        justDead.push_back(id);
                    }
                }
            }

            for(int id:justDead){
                std::cout<<"coordinator worker#"<<id<<" heartbeat timeout"<<std::endl;
                //节点失效，重新分配任务
            }
        }
    }

    void stopCoordinator(){
        rpcServer.stopServer();
        stop=true;
        rpcLoop.join();
        mLoop.join();
    }

    //信号触发退出回收套接字
    void handle_sigint(){
        int signo;
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask,SIGTSTP);
        sigaddset(&mask,SIGQUIT);

        if(pthread_sigmask(SIG_BLOCK,&mask,nullptr)!=0){
            perror("error setting signal mask");
            return;
        }

        while(!stop.load()){
            if(sigwait(&mask,&signo)!=0){
                perror("error in sigwait");
            }
            std::cout<<"signal coming"<<std::endl;
            if(signo==SIGTSTP||signo==SIGQUIT){
                std::cout<<"signal shutdown"<<std::endl;
                stopCoordinator();
            }
        }
    }

    std::map<std::string,std::string> run(){
        //生成中间键值对并分区
        std::vector<std::vector<MapKV>> partitions(nReduce);
        std::mutex part_mtx;

        std::vector<std::thread> mapThreads;
        for(auto &in:inputs){
            mapThreads.emplace_back([&,file=in.first,content=in.second]{
                auto kvs=mapfn(file,content);

                std::vector<std::vector<MapKV>> local(nReduce);
                for(auto& kv:kvs){
                    size_t b=std::hash<std::string>()(kv.first);
                    local[b].push_back(std::move(kv));
                }

                std::lock_guard<std::mutex> lk(part_mtx);
                for(int b=0;b<nReduce;b++){
                    auto &dst=partitions[b];
                    auto &src=local[b];
                    dst.insert(dst.end(),make_move_iterator(src.begin()),make_move_iterator(src.end()));
                }
            });
        }

        for(auto &t:mapThreads){
            t.join();
        }

        std::map<std::string,std::string> finalOut;  //最终输出
        std::mutex out_mtx;
        std::vector<std::thread> reduceThreads;

        for(int b=0;b<nReduce;++b){
            reduceThreads.emplace_back([&,b]{
                std::unordered_map<std::string,std::vector<std::string>> groups;
                groups.reserve(partitions[b].size()*2+1);
                for(auto& kv:partitions[b]){
                    groups[kv.first].push_back(kv.second);
                }

                std::vector<std::pair<std::string,std::string>> localResults;
                localResults.reserve(groups.size());
                for(auto &g:groups){
                    std::string r=reducefn(g.first,g.second);
                    localResults.emplace_back(g.first,std::move(r));
                }

                std::lock_guard<std::mutex> lk(out_mtx);
                for(auto &kv:localResults){
                    finalOut[kv.first]=std::move(kv.second);
                }
            });
        }

        for(auto &t:reduceThreads){
            t.join();
        }

        return finalOut;
    }
};

std::vector<MapKV> WordCountMap(const std::string& filename,const std::string& content){
    std::vector<MapKV> out;
    std::string w;
    out.reserve(content.size()/4+1);
    auto flush=[&]{
        if(!w.empty()){
            out.emplace_back(w,"1");
            w.clear();
        }
    };
    for(unsigned char ch:content){
        if(isalpha(ch)){
            w.push_back((char)tolower(ch));
        }else{
            flush();
        }
    }
    flush();
    return out;
}

std::string WordCountReduce(const std::string& key,const std::vector<std::string>& values){
    long long s=0;
    for(auto &v:values){
        s+=(v=="1")?1:stoll(v);
    }
    return std::to_string(s);
}

// int main(){
//     std::vector<std::pair<std::string,std::string>> inputs={
//         {"a.txt","hello world hello"},
//         {"b.txt","map reduce map mapreduce"},
//         {"c.txt","World of distributed map reduce HELLO"}
//     };

//     Coordinator coord{
//         3,
//         inputs,
//         WordCountMap,
//         WordCountReduce
//     };

//     auto result=coord.run();
//     std::cout<<"===final word count====\n";
//     for(auto& kv:result){
//         std::cout<<kv.first<<":"<<kv.second<<std::endl;
//     }
//     return 0;
// }