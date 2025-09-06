#pragma once
#include<cstdint>
#include<vector>
#include<string>
#include<optional>
#include<future>
#include<memory>

constexpr int kNilNode=-1;
enum class Role{Follower,Candidate,Leader};
struct LogEntry{
    uint64_t index{0};
    uint64_t term{0};
    std::string command;
};

struct Status{
    bool ok{true};
    std::string msg;
};

struct ReadResult{
    bool ok{true};
    std::string value;
};

struct RequestVoteReq{
    uint64_t term{0};
    int candidate{kNilNode};
    uint64_t lastLogIndex{0};
    uint64_t lastLogTerm{0};
};

struct RequestVoteResp{
    uint64_t term{0};
    bool voteGranted{false};
};

struct AppendEntriesReq{
    uint64_t term{0};
    int leaderId{kNilNode};
    uint64_t prevLogIndex{0};
    uint64_t prevLogTerm{0};
    std::vector<LogEntry> entries;
    uint64_t leaderCommit{0};
    std::optional<std::string> readCtx;
};

struct AppendEntriesResp{
    uint64_t term{0};
    bool success{false};
    uint64_t conflictIndex{0};
    int64_t conflictTerm{-1};
    bool readCtxAck{false};
};

struct InstallSnapshotReq{
    uint64_t term{0};
    int leaderId{kNilNode};
    uint64_t lastIncludeIndex{0};
    uint64_t lastIncludeTerm{0};
    uint64_t offset{0};
    std::string data;
    bool done{false};
};

struct InstallSnapshotResp{
    uint64_t term{0};
};

