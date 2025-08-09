#include"master.hpp"
#include"common.h"
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
#include<csignal>

using namespace std::chrono;

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

class Worker{
public:
    Worker(std::string addr,int hbIntervalMs)
    :addr(addr),hbIntervalMs(hbIntervalMs),stop(false),id(0),R(0),rpcPort(9900){}

    void start(){
        rpcServer.register_function("recoverTask",this,&Worker::recoverTask);
        rpcServer.register_function("copyFiles",this,&Worker::copyFiles);
        rpcServer.register_function("copyFileFromWorker",this,&Worker::copyFileFromWorker);
        rpcLoop=std::thread([this]{this->rpcServer.startRpcLoop(9900);});//开启循环
        rpcServer.startRpcLoop(9900);
        serverSock=connectNode("127.0.0.1",9099);
        json j=request(serverSock,"registerWorker","localhost");//向maser注册
        std::cout<<"result:"<<j["result"]<<std::endl;
        auto result=j["result"].get<std::pair<int,int>>();
        id=result.first;
        R=result.second;

        hbThread=std::thread(&Worker::heartbeatLoop,this);//启动心跳线程
        taskThread=std::thread(&Worker::taskLoop,this);//启动工作线程
        handle_sigint();
        if(hbThread.joinable()){
            hbThread.join();
        }
        if(taskThread.joinable()){
            taskThread.join();
        }
    }

    void setStatus(const std::string& s,int runningTask=-1){
        std::lock_guard<std::mutex> lk(mu);
        status=s;
        this->runningTask=runningTask;
    }

    //模拟崩溃
    void simulateCrash(){
        stop.store(true);
        std::cout<<"worker#"<<id<<" crashed (stop heartbeats)\n";
    }

    //正常关闭
    void shutdown(){
        rpcServer.stopServer();
        stop.store(true);
        if(hbThread.joinable()){
            hbThread.join();
        }
        if(taskThread.joinable()){
            taskThread.join();
        }
        if(rpcLoop.joinable()){
            rpcLoop.join();
        }
        close(serverSock);
        std::cout<<"worker#"<<id<<" shutdown\n";
    }

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
                shutdown();
            }
        }
    }

    //获取本节点文件
    std::string getFile(std::string filepath){
        //进行一些ip端口的检查等
        return readFile(filepath);
    }

    //重新执行map或reduce任务 
    int recoverTask(Task task){
        if(task.type==MAP){
            task.files[0].content=getNodeFile(task.files[0].filepath,serverSock);
            std::vector<std::string> files(R);
            processMapAndWrite(task.id,task.files[0].content,R,files);
            std::vector<FileItem> fileItems(R);
            for(int i=0;i<files.size();i++){
                fileItems[i]=makeFileItem(files[i]);
            }
            request(taskSock,"mapReport","127.0.0.1",9099,id,task.id,fileItems);//上报map任务完成
            std::cout<<"map report:"<<id<<"--"<<task.id<<std::endl;
        }else{
            std::string filepath=processReduceAndWrite(task,id);
            FileItem file=makeFileItem(filepath);
            json j=request(taskSock,"reduceReport","127.0.0.1",9099,id,task.id,file);//上报reduce任务完成
            std::cout<<"reduce report:"<<id<<"--"<<task.id<<std::endl;
        }
        return 0;
    }

    std::string copyFileFromWorker(FileItem file){
        std::string key=file.addr+":"+std::to_string(file.port);
        if(fileSocks.find(key)==fileSocks.end()){
            int fileSock=connectNode(file.addr,file.port);
            fileSocks[key]=fileSock;
        }
        std::string content=getNodeFile(file.filepath,fileSocks[key]);
        return content;
    }

    std::vector<MapFile> copyFiles(std::vector<MapFile> files){
        std::vector<MapFile> ret;
        for(auto& it:files){
            std::string content=copyFileFromWorker(it.file);
            writeFile(content,it.file.filepath);
            MapFile mf;
            mf.file.filepath=it.file.filepath;
            mf.file.addr=addr;
            mf.file.port=rpcPort;
            mf.addr=addr;
            mf.port=rpcPort;
            mf.workerID=id;
            ret.push_back(mf);
        }
        return ret;
    }


private:
    std::string addr;
    int hbIntervalMs;
    std::atomic<bool> stop;
    std::thread hbThread;//心跳线程
    std::thread taskThread;//任务线程
    int id;
    int R;
    std::mutex mu;
    std::string status="Idle";
    int runningTask=-1;
    int serverSock;
    int taskSock;
    std::atomic<bool> isReduce=false;
    RPCServer rpcServer;
    std::thread rpcLoop;
    int rpcPort;
    std::unordered_map<std::string,int> fileSocks;

    FileItem makeFileItem(std::string filepath){
        FileItem file;
        file.addr=addr;
        file.port=rpcPort;
        file.filepath=filepath;
        return file;
    }

    void registerRpc(){
        rpcServer.register_function("recoverTask",this,&Worker::recoverTask);
        rpcServer.register_function("copyFiles",this,&Worker::copyFiles);
        rpcServer.register_function("copyFileFromWorker",this,&Worker::copyFileFromWorker);
        rpcServer.register_function("getFile",this,&Worker::getFile);
    }

    void heartbeatLoop(){
        while(!stop.load()){
            {
                //std::lock_guard<std::mutex> lk(mu);
                json j=request(serverSock,"heartbeat",id,status,runningTask);
                bool ok=j["result"];
                if(!ok){
                    std::cout<<"worker#"<<id<<" heartbeat rejected (not registered?)\n";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(hbIntervalMs));
        }
    }

    void taskLoop(){
        taskSock=connectNode("127.0.0.1",9099);
        while(!stop.load()){
            json j=request(taskSock,"assignTask",id);
            Task task=j["result"].get<Task>();
            if(task.type==REDUCE){ //是reduce任务
                if(task.id==-1){
                    std::cout<<"no reduce task"<<std::endl;
                    std::this_thread::sleep_for(std::chrono::milliseconds(hbIntervalMs));
                    continue;
                }
                std::string filepath=processReduceAndWrite(task,id);
                j=request(taskSock,"reduceReport","127.0.0.1",9099,id,task.id,filepath);//上报reduce任务完成
                std::cout<<"reduce report:"<<id<<"--"<<task.id<<std::endl;
            }else{ //是map任务
                if(task.id!=-1){
                    std::cout<<"solving task "<<task.id<<" "<<task.files[0].filepath<<std::endl;
                    task.files[0].content=getNodeFile(task.files[0].filepath,serverSock);
                    std::vector<std::string> files(R);
                    processMapAndWrite(task.id,task.files[0].content,R,files);
                    j=request(taskSock,"mapReport","127.0.0.1",9099,id,task.id,files);//上报map任务完成
                    std::cout<<"map report:"<<id<<"--"<<task.id<<std::endl;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(hbIntervalMs));
        }
    }

    int connectNode(const std::string& ip,const int port){
        int sock=0;
        sockaddr_in serv_addr;
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

    //reduce执行与写入
    std::string processReduceAndWrite(Task& task,int workerId){
        std::cout<<"reduceId: "<<task.id<<std::endl;
        for(auto& it:task.files){
            std::cout<<"filepath: "<<it.filepath<<std::endl;
        }

        auto& files = task.files;
        std::vector<KeyValue> keyvals;
        std::string key, val;
        for (uint i = 0; i < files.size(); i++) {
            std::string content=copyFileFromWorker(files[i]);
            std::stringstream ss(content);
            while (ss >> key >> val) {
                keyvals.push_back({ key, val });
            }
        }

        std::sort(keyvals.begin(),keyvals.end());

        std::string preKey = keyvals[0].first;
        std::vector<std::string> vals;
        std::vector<KeyValue> ans;
        for (uint i = 0; i < keyvals.size(); i++) {
            if (preKey != keyvals[i].first) {
                std::vector<std::string> rs = Reduce(preKey, vals);
                if (!rs.empty()) {
                    ans.push_back({ preKey, rs[0] });
                }
                vals.clear();
                preKey = keyvals[i].first;
                vals.push_back(keyvals[i].second);
            } else {
                vals.push_back(keyvals[i].second);
            }
        }
        std::vector<std::string> rs = Reduce(preKey, vals);
        if (!rs.empty()) {
            ans.push_back({ preKey, rs[0] });
        }
        std::string filepath=writeReduceAnsToFile(task.id,ans);
        return filepath;
    }
};

int main(){
    Worker worker("127.0.0.1",1000);
    worker.start();
    return 0;
}
