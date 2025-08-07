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
using json=nlohmann::json;

const int REDUCE_FLAG=-2;
using MapKV=std::pair<std::string,std::string>;
using MapFn=std::function<std::vector<MapKV>(const std::string&,const std::string&)>;
using ReduceFn=std::function<std::string(const std::string&,const std::vector<std::string>&)>;
using KeyValue=std::pair<std::string,std::string>;

const int MAP=1;
const int REDUCE=2;
struct Task{
    int type;
    int id;
    std::vector<std::string> files;
};

// 序列化：Task -> json
inline void to_json(json& j, const Task& task) {
    j = json{
        {"type", task.type},
        {"id", task.id},
        {"files", task.files}
    };
}

// 反序列化：json -> Task
inline void from_json(const json& j, Task& task) {
    j.at("type").get_to(task.type);
    j.at("id").get_to(task.id);
    j.at("files").get_to(task.files);
}

struct MapFile{
    int workerID;
    std::string addr;
    int port;
    std::string filepath;
};

// 序列化：MapFile -> json
inline void to_json(json& j, const MapFile& mf) {
    j = json{
        {"workerID", mf.workerID},
        {"addr", mf.addr},
        {"port", mf.port},
        {"filepath", mf.filepath}
    };
}

// 反序列化：json -> MapFile
inline void from_json(const json& j, MapFile& mf) {
    j.at("workerID").get_to(mf.workerID);
    j.at("addr").get_to(mf.addr);
    j.at("port").get_to(mf.port);
    j.at("filepath").get_to(mf.filepath);
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

//reduce执行与写入
std::string processReduceAndWrite(Task& task,int workerId,int taskSock){
    std::cout<<"reduceId: "<<task.id<<std::endl;
    for(auto& it:task.files){
        std::cout<<"filepath: "<<it<<std::endl;
    }

    auto& files = task.files;
    std::vector<KeyValue> keyvals;
    std::string key, val;
    for (uint i = 0; i < files.size(); i++) {
        std::ifstream ifs(files[i]);
        while (ifs >> key >> val) {
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

void mergeFiles(const std::vector<std::string>& inputFiles, const std::string& outputFile) {
    std::ofstream out(outputFile, std::ios::out | std::ios::binary);
    if (!out) {
        std::cerr << "Cannot open output file: " << outputFile << std::endl;
        return;
    }

    for (const auto& file : inputFiles) {
        std::ifstream in(file, std::ios::in | std::ios::binary);
        if (!in) {
            std::cerr << "Cannot open input file: " << file << std::endl;
            continue; // Skip this file
        }

        out << in.rdbuf(); // 将输入文件内容直接写入输出文件
        in.close();
    }

    out.close();
    std::cout << "Merged " << inputFiles.size() << " files into " << outputFile << std::endl;
}