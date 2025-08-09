#pragma once
#include"json.hpp"
#include<iostream>
#include<fstream>
#include<vector>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<unistd.h>
#include<cstring>
#include<csignal>
#include<filesystem>
#include<sstream>

using json=nlohmann::json;
namespace fs=std::filesystem;
const int REDUCE_FLAG=-2;
using MapKV=std::pair<std::string,std::string>;
using MapFn=std::function<std::vector<MapKV>(const std::string&,const std::string&)>;
using ReduceFn=std::function<std::string(const std::string&,const std::vector<std::string>&)>;
using KeyValue=std::pair<std::string,std::string>;

const int MAP=1;
const int REDUCE=2;
struct FileItem{
    std::string addr;
    int port;
    std::string filepath;
    std::string content;
};
struct Task{
    int type;
    int id;
    std::vector<FileItem> files;
};

// 先为 FileItem 定义
inline void to_json(json& j, const FileItem& f) {
    j = json{
        {"addr", f.addr},
        {"port", f.port},
        {"filepath", f.filepath},
        {"content",f.content}
    };
}

inline void from_json(const json& j, FileItem& f) {
    j.at("addr").get_to(f.addr);
    j.at("port").get_to(f.port);
    j.at("filepath").get_to(f.filepath);
    j.at("content").get_to(f.content);
}

// 再为 Task 定义（依赖于 FileItem 的定义）
inline void to_json(json& j, const Task& task) {
    j = json{
        {"type", task.type},
        {"id", task.id},
        {"files", task.files}  // 自动使用 FileItem 的 to_json
    };
}

inline void from_json(const json& j, Task& task) {
    j.at("type").get_to(task.type);
    j.at("id").get_to(task.id);
    j.at("files").get_to(task.files);  // 自动使用 FileItem 的 from_json
}

struct MapFile{
    int workerID;
    std::string addr;
    int port;
    FileItem file;
};

// 序列化：MapFile -> json
inline void to_json(json& j, const MapFile& mf) {
    j = json{
        {"workerID", mf.workerID},
        {"addr", mf.addr},
        {"port", mf.port},
        {"file", mf.file}  // 直接存储整个 FileItem
    };
}

// 反序列化：json -> MapFile
inline void from_json(const json& j, MapFile& mf) {
    j.at("workerID").get_to(mf.workerID);
    j.at("addr").get_to(mf.addr);
    j.at("port").get_to(mf.port);
    j.at("file").get_to(mf.file);  // 直接解析成 FileItem
}

// 简单的 Map 函数，统计单词出现次数
void combiner(std::vector<std::pair<std::string,int>>& kvs){
    std::unordered_map<std::string,int> counts;
    for(auto& kv:kvs){
        counts[kv.first]+=kv.second;
    }
    kvs.clear();
    for(const auto& kv:counts){
        kvs.push_back(kv);
    }
}

std::vector<std::pair<std::string, int>> Map(const std::string& text) {
    std::vector<std::pair<std::string, int>> kvs;
    std::string word;
    for (char ch : text) {
        if (isalpha(ch)) {
            word += tolower(ch);  // 小写化
        } else if (!word.empty()) {
            kvs.push_back({word, 1});
            word.clear();
        }
    }
    if (!word.empty()) kvs.push_back({word, 1});
    combiner(kvs);
    return kvs;
}

std::vector<std::string> Reduce(const std::string& key,const std::vector<std::string> vals){
    int num=0;
    for(auto& it:vals){
        num+=atoi(it.c_str());
    }
    return {std::to_string(num)};
}

int partition(const std::string& key,int R){
    return std::hash<std::string>{}(key)%R;
}

void writePartitionToFile(int mapID,int reduceID,const std::vector<std::pair<std::string,int>>& kvs,std::vector<std::string>& files){
    std::string filename = "./reduce/mr-" +std::to_string(mapID)+"-"+std::to_string(reduceID);
    files[reduceID]=filename;
    std::ofstream outfile(filename,std::ios::out|std::ios::app);
    for(const auto& kv:kvs){
        outfile<<kv.first<<" "<<kv.second<<"\n";
    }
    outfile.close();
}

std::string writeReduceAnsToFile(int reduceID,const std::vector<KeyValue>& ans){
    std::string filename = "./reduce/rd-" +std::to_string(reduceID);
    std::ofstream outfile(filename,std::ios::out|std::ios::app);
    for(const auto& kv:ans){
        outfile<<kv.first<<" "<<kv.second<<"\n";
    }
    outfile.close();
    return filename;
}

//map执行与分区写入
void processMapAndWrite(int mapID,const std::string& text,int R,std::vector<std::string>& files){
    //执行map得到键值对
    auto kvs=Map(text);
    std::vector<std::vector<std::pair<std::string,int>>> partitions(R);

    for(const auto& kv:kvs){
        int partitionID=partition(kv.first,R);//根据key分配分区
        partitions[partitionID].push_back(kv);
    }

    for(int i=0;i<R;i++){
        if(!partitions[i].empty()){
            writePartitionToFile(mapID,i,partitions[i],files);
        }
    }
}

template<typename... Args>
json request(int sock,const std::string& name,Args... args){
    char buffer[1024]={0};
    std::string req=make_request(name,args...);
    std::cout<<"req:"<<req<<std::endl;
    send(sock,req.c_str(),req.size(),0);
    int bytes=read(sock,buffer,1024);
    if(bytes<=0){
        json j;
        j["result"]=-1;
        return j;
    }
    std::string resp(buffer,bytes);
    json j=json::parse(resp);
    return j;
}

//文件读取函数
std::string readFile(std::string filename){
    std::ifstream file(filename);
    if (!file) {
        std::cerr << "无法打开文件\n";
        return "";
    }

    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());

    return content;
}

void writeFile(const std::string& content,const std::string& filename){
    // 打开输出文件流（默认是 std::ios::out）
    std::ofstream outFile(filename);
    if (!outFile) {
        std::cerr << "无法打开文件进行写入: " << filename << std::endl;
        return;
    }

    // 写入字符串内容
    outFile << content;

    // 可选：关闭文件（析构函数也会自动关闭）
    outFile.close();
}

//获取节点上的远程文件
std::string getNodeFile(std::string filepath,int sock){
    json j=request(sock,"getFile",filepath);
    std::string content=std::move(j["result"].get<std::string>());
    return content;
}