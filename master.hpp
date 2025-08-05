#pragma once
#include"rpc.hpp"
#include"common.h"
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

struct WorkerInfo{
    int id=0;
    std::string addr;
    steady_clock::time_point lastHB;//最近一次心跳时间
    std::string status="Idle";
    int runningTask=-1;
    bool alive=true;  //是否存活
    std::vector<Task> tasks;
    std::mutex tsk_mtx; //tasks锁
};

struct MapFile{
    int workerID;
    std::string addr;
    int port;
    std::string filepath;
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
    std::unordered_map<int,std::vector<MapFile>> mapOutFiles;  //每个mapId对应的mapFile列表
    std::unordered_map<int,std::vector<MapFile>> reduceInputFiles; //每个reduceId对应的mapFile列表
    std::vector<MapFile> reduceAnsFiles; //reduce结果文件
    std::mutex reduceMapMtx;
    std::mutex mu;
    std::mutex task_mtx;
    RPCServer rpcServer;
    std::thread rpcLoop,mLoop;
    const int PORT=9099;
    std::vector<Task> tasks;
    std::mutex mfMtx;
    std::mutex rdAnsMtx;
    std::atomic<int> mapCompleteCount;
    std::atomic<int> reduceCompleteCount;
    int totalMapTasks;
    std::atomic<bool> isReduce;

    std::pair<int,int> registerWorker(std::string addr){
        std::lock_guard<std::mutex> lk(mu);
        int id=nextId++;
        WorkerInfo &wi=workers[id];
        wi.id=id;
        wi.addr=addr;
        wi.lastHB=steady_clock::now();
        wi.status="Register";
        wi.alive=true;
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
        rpcServer.register_function("assignTask",this,&Coordinator::assignTask);
        rpcServer.register_function("mapReport",this,&Coordinator::mapReport);
    }

    MapFile testfunc(){
        MapFile mf;
        mf.addr="fdsf";
        mf.filepath="fdfs";
        mf.port=23;
        mf.workerID=34;
        return mf;
    }

    //分发map任务
    Task assignTask(int workerId){
        if(!isReduce.load()){
            std::lock_guard<std::mutex> lock(task_mtx);
            if(!tasks.empty()){
                auto task=tasks.begin();
                Task assignedTask=*task;
                tasks.erase(task);
                {
                    std::lock_guard<std::mutex> lck(workers[workerId].tsk_mtx);
                    workers[workerId].tasks.push_back(assignedTask);   //添加到worker的待完成任务队列中
                }
                return assignedTask;
            }else{
                return {MAP,-1,{}};
            }
        }else{
            //进入reduce阶段后分配reduce任务
            std::lock_guard<std::mutex> lock(reduceMapMtx);
            if(!reduceInputFiles.empty()){
                auto it=reduceInputFiles.begin();
                Task rt;
                rt.type=REDUCE;
                rt.id=it->first;
                for(auto& it:it->second){
                    rt.files.push_back(it.filepath);
                }
                reduceInputFiles.erase(it);
                return rt;
            }
            return {REDUCE,-1,{}};
        }
    }

    int finishMapTask(int workerId,int taskId){
        mapCompleteCount++;
        if(mapCompleteCount==totalMapTasks){
            startReduce(); //map任务全部完成,开启reduce阶段
        }
        return 0;
    }

    int finishReduceTask(int workerId,int taskId){
        reduceCompleteCount++;
        if(reduceCompleteCount==R){
            //归约reduce结果
            std::string resultfile="./reduce/result";
            std::vector<std::string> inputFiles;
            for(auto& it:reduceAnsFiles){
                inputFiles.push_back(it.filepath);
            }
            mergeFiles(inputFiles,resultfile);
        }
        return 0;
    }

    //节点map结果上报
    int mapReport(const std::string addr,int port,int workerID,int mapID,const std::vector<std::string> filepaths){
        std::vector<MapFile> mapFiles;
        for(auto& it:filepaths){
            MapFile mf;
            mf.addr=addr;
            mf.port=port;
            mf.workerID=workerID;
            mf.filepath=it;
            mapFiles.push_back(mf);
        }
        {
            std::lock_guard<std::mutex> lck(mfMtx);
            mapOutFiles[mapID]=std::move(mapFiles);
        }
        finishMapTask(workerID,mapID);
        return 0;
    }

    //节点reduce结果上报
    int reduceReport(const std::string addr,int port,int workerID,int reduceID,const std::string filepath){
        MapFile mf;
        mf.addr=addr;
        mf.port=port;
        mf.workerID=workerID;
        mf.filepath=filepath;

        {
            std::lock_guard<std::mutex> lck(rdAnsMtx);
            reduceAnsFiles.push_back(mf);
        }
        finishReduceTask(workerID,reduceID);
        return 0;
    }

public:
    explicit Coordinator(int R,int timeoutMs):R(R),timeoutMs(timeoutMs),nextId(1),stop(false),mapCompleteCount(0),
    isReduce(false){
        tasks={{MAP,0,{"file1.txt"}},{MAP,1,{"file2.txt"}},{MAP,2,{"file3.txt"}}};//初始化任务列表
        totalMapTasks=tasks.size();
    }

    //开启reduce阶段
    void startReduce(){
        //通知各个woker节点进入reduce阶段
        std::lock_guard<std::mutex> lck(reduceMapMtx);
        for(int i=0;i<R;i++){
            for(auto& it:mapOutFiles){
                reduceInputFiles[i].push_back(it.second[i]);
            }
        }
        isReduce=true;
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
