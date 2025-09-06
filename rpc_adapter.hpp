#pragma once
#include"types.hpp"
#include<functional>
#include<string>
#include<future>
#include<unordered_map>
#include<memory>

class IRpcAdapter{
public:
    virtual ~IRpcAdapter()=default;
    virtual void BindHandlers(
        std::function<void(const RequestVoteReq&,RequestVoteResp&)> onVote,
        std::function<void(const AppendEntriesReq&,AppendEntriesResp&)> onAppend,
        std::function<void(const InstallSnapshotReq&,InstallSnapshotResp&)> onInstall
    )=0;

    virtual std::future<RequestVoteResp> RequestVote(int peer,const RequestVoteReq& req)=0;
    virtual std::future<AppendEntriesResp> AppendEntries(int peer,const AppendEntriesReq& req)=0;
    virtual std::future<InstallSnapshotResp> InstallSnapshot(int peer,const InstallSnapshotReq& req)=0;
};

