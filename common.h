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
    return {std::to_string(vals.size())};
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