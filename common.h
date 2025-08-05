#pragma once
#include<iostream>
#include<fstream>
#include<vector>

const int REDUCE_FLAG=-2;
using Task=std::pair<int,std::string>;
using ReduceTask=std::pair<int,std::vector<std::string>>;  //reduceId和reduceFiles
using MapKV=std::pair<std::string,std::string>;
using MapFn=std::function<std::vector<MapKV>(const std::string&,const std::string&)>;
using ReduceFn=std::function<std::string(const std::string&,const std::vector<std::string>&)>;
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

int partition(const std::string& key,int R){
    return std::hash<std::string>{}(key)%R;
}

void writePartitionToFile(int mapID,int reduceID,const std::vector<std::pair<std::string,int>>& kvs,std::vector<std::pair<int,std::string>>& files){
    std::string filename = "./reduce/mr-" +std::to_string(mapID)+"-"+std::to_string(reduceID);
    files.push_back({reduceID,filename});
    std::ofstream outfile(filename,std::ios::out|std::ios::app);
    for(const auto& kv:kvs){
        outfile<<kv.first<<" "<<kv.second<<"\n";
    }
    outfile.close();
}

//map执行与分区写入
void processMapAndWrite(int mapID,const std::string& text,int R,std::vector<std::pair<int,std::string>>& files){
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