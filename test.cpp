// #include<iostream>
// #include"common.h"
// #include"rpc.hpp"

// int main(){
//     int sock=0;
//     sockaddr_in serv_addr;
//     char buffer[1024]={0};
//     sock=socket(AF_INET,SOCK_STREAM,0);
//     serv_addr.sin_family=AF_INET;
//     serv_addr.sin_port=htons(9099);

//     inet_pton(AF_INET,"127.0.0.1",&serv_addr.sin_addr);
//     int conn=connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
//     if(conn<0){
//         return -1;
//     }
//     std::string req=make_request("testfunc");
//     std::cout<<"req:"<<req<<std::endl;
//     send(sock,req.c_str(),req.size(),0);
//     int bytes=read(sock,buffer,1024);
//     if(bytes<=0){
//         json j;
//         j["result"]=-1;
//         return j;
//     }
//     std::string resp(buffer,bytes);
//     std::cout<<resp<<std::endl;
//     return 0;
// }

#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <cctype>

// 转小写并保留字母数字
std::string normalize(const std::string& word) {
    std::string result;
    for (char c : word) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            result += std::tolower(static_cast<unsigned char>(c));
        }
    }
    return result;
}

void wordCount(const std::vector<std::string>& filepaths, const std::string& outputFile) {
    std::unordered_map<std::string, int> wordFreq;

    for (const auto& filepath : filepaths) {
        std::ifstream in(filepath);
        if (!in) {
            std::cerr << "Failed to open file: " << filepath << std::endl;
            continue;
        }

        std::string line;
        while (std::getline(in, line)) {
            std::istringstream iss(line);
            std::string word;
            while (iss >> word) {
                std::string clean = normalize(word);
                if (!clean.empty()) {
                    wordFreq[clean]++;
                }
            }
        }

        in.close();
    }

    std::ofstream out(outputFile);
    if (!out) {
        std::cerr << "Failed to open output file: " << outputFile << std::endl;
        return;
    }

    for (const auto& [word, count] : wordFreq) {
        out << word << " " << count << "\n";
    }

    out.close();
    std::cout << "Word count written to " << outputFile << std::endl;
}

int main() {
    std::vector<std::string> files = {
        "file1.txt",
        "file2.txt",
        "file3.txt"
    };

    wordCount(files, "result.txt");
    return 0;
}
