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
#include <chrono>
#include <ctime>
#include<queue>
#include <condition_variable>
#include<memory>

using std::chrono::duration_cast;
using std::chrono::system_clock;

std::string unix_timestamp_str()
{
    // 当前时间点 → 自纪元以来的秒数
    auto seconds = duration_cast<std::chrono::seconds>(system_clock::now().time_since_epoch()).count();
    return std::to_string(seconds);
}

using namespace std::chrono;

struct WorkerInfo{
    int rpcSock;
    int id=0;
    std::string addr;
    steady_clock::time_point lastHB;//最近一次心跳时间
    std::string status="Idle";
    int runningTask=-1;
    bool alive=true;  //是否存活
    std::vector<Task> tasks;
    std::mutex tsk_mtx; //tasks锁
};

class Coordinator{
private:
    int nReduce;
    std::string addr;
    std::vector<std::pair<std::string,std::string>> inputs;
    MapFn mapfn;
    ReduceFn reducefn;
    int R;
    int timeoutMs;
    std::atomic<int> nextId;
    std::atomic<bool> stop;
    std::unordered_map<int,std::shared_ptr<WorkerInfo>> workers;
    std::unordered_map<int,std::vector<MapFile>> mapOutFiles;  //每个mapId对应的mapFile列表
    std::unordered_map<int,std::string> mapTimeStr; //每个mapId对应map任务完成的最新时间戳
    std::unordered_map<int,std::vector<MapFile>> reduceInputFiles; //每个reduceId对应的mapFile列表
    std::vector<MapFile> reduceAnsFiles; //reduce结果文件
    std::mutex reduceMapMtx;
    std::mutex mu;
    std::mutex task_mtx;
    std::shared_ptr<RPCServer> rpcServer;
    std::thread rpcLoop,mLoop,recoverLoop;
    const int PORT=9099;
    std::vector<Task> tasks;
    std::queue<Task> recoverTasks;
    int lastRecoverWorkId=-1;
    std::mutex recoverMtx;
    std::condition_variable not_empty;
    std::mutex mfMtx;
    std::mutex rdAnsMtx;
    std::atomic<int> mapCompleteCount;
    std::atomic<int> reduceCompleteCount;
    int totalMapTasks;
    std::atomic<bool> isReduce;
    std::unordered_map<std::string,std::vector<MapFile>> fileMap;
    std::mutex fileMapMtx;
    std::shared_ptr<ThreadPool> pool;

    std::pair<int,int> registerWorker(std::string addr){
        std::lock_guard<std::mutex> lk(mu);
        int id=nextId++;
        auto wi=workers[id];
        wi->id=id;
        wi->addr=addr;
        wi->lastHB=steady_clock::now();
        wi->status="Register";
        wi->alive=true;
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
        it->second->lastHB=steady_clock::now();
        it->second->status=status;
        it->second->runningTask=runningTask;
        if(!it->second->alive){
            it->second->alive=true;
            std::cout<<"coordinator worker#"<<workerId<<" revived by heartbeat"<<std::endl;
        }
        return true;
    }

    void rpcRegister(){
        rpcServer->register_function("registerWorker",this,&Coordinator::registerWorker);
        rpcServer->register_function("heartbeat",this,&Coordinator::heartbeat);
        rpcServer->register_function("assignTask",this,&Coordinator::assignTask);
        rpcServer->register_function("mapReport",this,&Coordinator::mapReport);
        rpcServer->register_function("reduceReport",this,&Coordinator::reduceReport);
        rpcServer->register_function("getFile",this,&Coordinator::getFile);
    }

    void assignRecoverTask(){
        while(!stop.load()){
            std::unique_lock<std::mutex> lk(recoverMtx);
            not_empty.wait(lk,[this]{return !this->recoverTasks.empty();});
            Task task=this->recoverTasks.front();
            this->recoverTasks.pop();
            lk.unlock();

            auto worker_ptr=getAliveWorker();
            if(worker_ptr==nullptr){
                perror("can not find alive worker\n");
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }

            std::unique_lock<std::mutex> lck(worker_ptr->tsk_mtx);
            worker_ptr->tasks.push_back(task);
            int taskSock=worker_ptr->rpcSock;
            lck.unlock();
            json j=request(taskSock,"recoverTask",task);
            std::cout<<"assign recoverTask taskId:"<<task.id<<" taskType:"<<task.type<<" workerId:"<<worker_ptr->id<<std::endl;
        }
    }

    //分发map任务
    Task assignTask(int workerId){
        if(!isReduce.load()){
            std::lock_guard<std::mutex> lock(task_mtx);
            if(!tasks.empty()){
                auto task=tasks.begin();
                Task assignedTask=*task;
                assignedTask.type=MAP;
                tasks.erase(task);
                {
                    std::lock_guard<std::mutex> worklk(mu);
                    std::lock_guard<std::mutex> lck(workers[workerId]->tsk_mtx);
                    workers[workerId]->tasks.push_back(assignedTask);   //添加到worker的待完成任务队列中
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
                    rt.files.push_back(it.file);
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
            std::vector<FileItem> inputFiles;
            {
                std::lock_guard<std::mutex> lck(rdAnsMtx);
                for(auto& it:reduceAnsFiles){
                    inputFiles.push_back(it.file);
                }
            }
            mergeFiles(inputFiles,resultfile,workers[workerId]->rpcSock);
        }
        return 0;
    }

    //获取本节点文件
    std::string getFile(std::string filepath){
        //进行一些ip端口的检查等
        return readFile(filepath);
    }

    //节点map结果上报
    int mapReport(const std::string addr,int port,int workerID,int mapID,const std::vector<FileItem> files){
        auto mapFiles=std::make_shared<std::vector<MapFile>>();
        for(auto& it:files){
            MapFile mf;
            mf.addr=addr;
            mf.port=port;
            mf.workerID=workerID;
            mf.file=it;
            mapFiles->push_back(mf);
        }

        {
            std::lock_guard<std::mutex> lck(fileMapMtx);
            for(auto& it:*mapFiles){
                fileMap[it.file.filepath].push_back(it);
            }
        }

        {
            std::lock_guard<std::mutex> lck(mfMtx);
            mapOutFiles[mapID]=std::move(*mapFiles);
            mapTimeStr[mapID]=unix_timestamp_str();   
        }
        finishMapTask(workerID,mapID);

        auto worker_ptr=getAliveWorker();
        if(worker_ptr==nullptr){
            perror("can not find alive worker\n");
            return 0;
        }
        if(worker_ptr->id==workerID){
            worker_ptr=getAliveWorker();
            if(worker_ptr->id==workerID){
                return 0; //找不到第二个工作节点
            }
        }

        json j=request(worker_ptr->rpcSock,"copyFiles",*mapFiles);
        std::vector<MapFile> v=j["result"].get<std::vector<MapFile>>();
        {
            std::lock_guard<std::mutex> lck(fileMapMtx);
            for(auto& it:v){
                fileMap[it.file.filepath].push_back(it);
            }
        }
        return 0;
    }

    //节点reduce结果上报
    int reduceReport(const std::string addr,int port,int workerID,int reduceID,FileItem file){
        MapFile mf;
        mf.addr=addr;
        mf.port=port;
        mf.workerID=workerID;
        mf.file=file;

        {
            std::lock_guard<std::mutex> lck(rdAnsMtx);
            reduceAnsFiles.push_back(mf);
        }
        finishReduceTask(workerID,reduceID);
        return 0;
    }

public:
    explicit Coordinator(int R,int timeoutMs):R(R),timeoutMs(timeoutMs),nextId(1),stop(false),mapCompleteCount(0),
    isReduce(false),addr("127.0.0.1"){
        tasks={{MAP,0,{makeFileItem("file1.txt")}},{MAP,1,{makeFileItem("file2.txt")}},{MAP,2,{makeFileItem("file3.txt")}}};//初始化任务列表
        totalMapTasks=tasks.size();
    }

    std::shared_ptr<WorkerInfo> getAliveWorker(){
        std::lock_guard<std::mutex> worklk(mu);
        int workerNum=workers.size();
        int num=0;
        int workerId=0;
        for(int i=lastRecoverWorkId+1;num<=workerNum;i++,num++){
            workerId=i%workerNum;
            {
                if(workers[workerId]->alive){
                    break;
                }
            }
        }

        if(num==workerNum){
            perror("can not find alive worker\n");
            return nullptr;
        }
        lastRecoverWorkId=(lastRecoverWorkId+num)%workerNum;
        return workers[workerId];
    }

    FileItem makeFileItem(std::string filepath){
        FileItem file;
        file.addr=addr;
        file.port=PORT;
        file.filepath=filepath;
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
        pool=std::make_shared<ThreadPool>(5);
        rpcServer=std::make_shared<RPCServer>(pool);
        rpcRegister();//注册rpc
        rpcLoop=std::thread([this]{this->rpcServer->startRpcLoop(PORT);});//开启循环
        mLoop=std::thread([this]{this->monitorLoop();});
        recoverLoop=std::thread([this]{this->assignRecoverTask();});
        handle_sigint();
    }

    void monitorLoop(){
        while(!stop.load()){
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            std::vector<int> justDead;
            {
                std::lock_guard<std::mutex> lk(mu);
                auto now=steady_clock::now();
                for(auto [id,w]: workers){
                    auto ms=duration_cast<milliseconds>(now-w->lastHB).count();
                    if(w->alive&&ms>timeoutMs){
                        w->alive=false;
                        justDead.push_back(id);
                    }
                }
            }

            for(int id:justDead){
                std::cout<<"coordinator worker#"<<id<<" heartbeat timeout"<<std::endl;
                //节点失效，重新分配任务
                std::vector<Task> tasks;
                {
                    std::lock_guard<std::mutex> worklk(mu);
                    std::lock_guard<std::mutex> lck(workers[id]->tsk_mtx);
                    tasks=workers[id]->tasks;
                }
                std::unique_lock<std::mutex> lck(recoverMtx);
                for(auto& task:tasks){
                    bool was_empty=recoverTasks.empty();
                    recoverTasks.push(task);
                    lck.unlock(); //先解锁再通知
                    if(was_empty){
                        not_empty.notify_one();
                    }
                }
            }
        }
    }

    void stopCoordinator(){
        rpcServer->stopServer();
        stop=true;
        rpcLoop.join();
        mLoop.join();
        recoverLoop.join();
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

    void mergeFiles(const std::vector<FileItem>& inputFiles, const std::string& outputFile, int sock) {
        std::ofstream out(outputFile, std::ios::out | std::ios::binary);
        if (!out) {
            std::cerr << "Cannot open output file: " << outputFile << std::endl;
            return;
        }

        for (const auto& file : inputFiles) {
            json j=request(sock,"getFile",file);
            std::string content=std::move(j["result"].get<std::string>());
            out.write(content.data(),content.size());
        }

        out.close();
        std::cout << "Merged " << inputFiles.size() << " files into " << outputFile << std::endl;
    }
};
