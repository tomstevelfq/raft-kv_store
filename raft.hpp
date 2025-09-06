#pragma once
#include"types.hpp"
#include"storage.hpp"
#include"sm.hpp"
#include"timer.hpp"
#include"rpc_adapter.hpp"
#include<atomic>
#include<mutex>
#include<condition_variable>

class IRpcAdapter;
class RaftNode{
public:
    RaftNode(int my_id,std::vector<int>peers,std::shared_ptr<IRaftStorage> storage,
            std::shared_ptr<IRpcAdapter> rpc,std::shared_ptr<IStateMachine> sm,std::shared_ptr<ITimer> timer);
    
    std::future<Status> Propose(const std::string& cmd);
    std::future<ReadResult> ReadIndex(const std::string& ctx);

    void Start();
    void Stop();

    void HandleRequestVote(const RequestVoteReq& req, RequestVoteResp& resp);
    void HandleAppendEntries(const AppendEntriesReq& req,AppendEntriesResp& resp);
    void HandleInstallSnapshot(const InstallSnapshotReq& req,InstallSnapshotResp& resp);

private:
    void becomeFollower(uint64_t newTerm,int leader=kNilNode);
    void becomeCandidate();
    void becomeLeader();

    void electionTick();
    void heartbeatTick();


    void replicateTo(int peer);
    void onAppendResp(int peert,const AppendEntriesResp& r);

    void advanceCommitIndex();
    void applyLoop();

    const LogEntry* entry(uint64_t idx) const;
    bool logUpToDate(uint64_t lastLogIndex,uint64_t lastLogTerm) const;

    const int id_;
    const std::vector<int> peers_;
    std::shared_ptr<IRaftStorage> storage_;
    std::unique_ptr<ILog> log_;
    std::unique_ptr<ISnapshotStore> snaps_;
    std::shared_ptr<IRpcAdapter> rpc_;
    std::shared_ptr<IStateMachine> sm_;
    std::shared_ptr<ITimer> timer_;

    std::atomic<Role> role_{Role::Follower};
    std::atomic<uint64_t> currentTerm_{0};
    std::atomic<int> votedFor_{kNilNode};

    std::atomic<uint64_t> commitIndex_{0};
    std::atomic<uint64_t> lastApplied_{0};
    std::vector<uint64_t> nextIndex_;
    std::vector<uint64_t> matchIndex_;

    std::atomic<int> leaderId_{kNilNode};

    std::mutex mtx_;
    std::condition_variable applyCv_;
    std::atomic<bool> running_{false};
};

std::shared_ptr<IRpcAdapter> MakeRpcAdapter(int my_id,const std::vector<int>& peers, const std::string& listen_addr);

RaftNode::RaftNode(int my_id,std::vector<int> peers,std::shared_ptr<IRaftStorage> storage,std::shared_ptr<IRpcAdapter> rpc,
                   std::shared_ptr<IStateMachine> sm,std::shared_ptr<ITimer> timer):id_(my_id),peers_(std::move(peers)),storage_(std::move(storage)),rpc_(std::move(rpc)),sm_(std::move(sm)),timer_(std::move(timer)){
        log_=storage_->OpenLog();
        snaps_=storage_->OpenSnapshot();
        nextIndex_.resize(peers_.size(),log_->LastIndex()+1);
        matchIndex_.resize(peers_.size(),0);
}

std::future<Status> RaftNode::Propose(const std::string& cmd){
    std::promise<Status> p;
    p.set_value(Status{false,"TODO: Propose not implemented"});
    return p.get_future();
}

std::future<ReadResult> RaftNode::ReadIndex(const std::string& ctx){
    std::promise<ReadResult> p;
    p.set_value(ReadResult{false,"TODO: ReadIndex not implemented"});
    return p.get_future();
}

void RaftNode::Start(){
    running_=true;
    rpc_->BindHandlers(
        [this](const RequestVoteReq& req,RequestVoteResp& resp){this->HandleRequestVote(req,resp);},
        [this](const AppendEntriesReq& req,AppendEntriesResp& resp){this->HandleAppendEntries(req,resp);},
        [this](const InstallSnapshotReq& req,InstallSnapshotResp& resp){this->HandleInstallSnapshot(req,resp);}
    );

    //

    std::thread([this]{applyLoop();}).detach();
}

void RaftNode::Stop(){
    running_=false;
    applyCv_.notify_all();
}

void RaftNode::HandleRequestVote(const RequestVoteReq& req, RequestVoteResp& resp){
    resp.term=currentTerm_;
    resp.voteGranted=false;
    if(req.term<currentTerm_)return;
    if(req.term>currentTerm_){
        becomeFollower(req.term,kNilNode);
    }

    if(logUpToDate(req.lastLogIndex,req.lastLogTerm)){
        votedFor_=req.candidate;
        resp.voteGranted=true;
    }
}

void RaftNode::HandleAppendEntries(const AppendEntriesReq& req,AppendEntriesResp& resp){
    resp.term=currentTerm_;
    resp.success=false;
    if(req.term<currentTerm_){
        return;
    }

    if(req.term>currentTerm_){
        becomeFollower(req.term,req.leaderId);
    }

    leaderId_=req.leaderId;
    //
    resp.success=true;
}

void RaftNode::HandleInstallSnapshot(const InstallSnapshotReq& req,InstallSnapshotResp& resp){
    resp.term=currentTerm_;
    if(req.term<currentTerm_){
        return;
    }
    if(req.term>currentTerm_){
        becomeFollower(req.term,req.leaderId);
    }
    //
}

void RaftNode::becomeFollower(uint64_t newTerm,int leader){
    currentTerm_=newTerm;
    votedFor_=kNilNode;
    role_=Role::Follower;
    leaderId_=leader;
    //
}

void RaftNode::becomeCandidate(){
    role_=Role::Candidate;
    currentTerm_=currentTerm_+1;
    votedFor_=id_;
    //
}

void RaftNode::becomeLeader(){
    role_=Role::Leader;
    
    std::fill(nextIndex_.begin(),nextIndex_.end(),log_->LastIndex()+1);
    std::fill(matchIndex_.begin(),matchIndex_.end(),0);
    //
}

void RaftNode::electionTick(){

}

void RaftNode::heartbeatTick(){

}

void RaftNode::replicateTo(int peer){

}

void RaftNode::onAppendResp(int peer,const AppendEntriesResp& resp){

}

void RaftNode::advanceCommitIndex(){

}

void RaftNode::applyLoop(){
    while(running_){
        std::unique_lock lk(mtx_);
        applyCv_.wait(lk,[this]{return !running_||lastApplied_<commitIndex_;});
        if(!running_) break;
        //
    }
}

const LogEntry* RaftNode::entry(uint64_t idx)const{
    return nullptr;
}

bool RaftNode::logUpToDate(uint64_t lastLogIndex,uint64_t lastLogTerm)const{
    const auto myLastIdx=log_->LastIndex();
    const auto myLastTerm=log_->Term(myLastIdx);
    if(lastLogTerm!=myLastTerm) return lastLogTerm>myLastTerm;
    return lastLogIndex>=myLastIdx;
}