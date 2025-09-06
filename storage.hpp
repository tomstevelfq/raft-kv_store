#pragma once
#include "types.hpp"
#include<utility>

class ILog{
public:
    virtual~ILog()=default;
    virtual uint64_t FirstIndex() const =0;
    virtual uint64_t LastIndex() const=0;
    virtual uint64_t Term(uint64_t index) const=0;
    virtual std::vector<LogEntry> Slice(uint64_t lo,uint64_t hi)=0;
    virtual void Append(std::vector<LogEntry>&& entries)=0;
    virtual void TruncateShffix(uint64_t from)=0;
};

class ISnapshotStore{
public:
    virtual ~ISnapshotStore()=default;
    struct Meta{
        uint64_t lastIncludedIndex{0};
        uint64_t lastIncludedTerm{0};
    };
    struct Data{
        Meta meta;
        std::string blob;
    };
    virtual void Save(const Data& snap)=0;
    virtual Data LoadLatest()=0;
};

class IRaftStorage{
public:
    virtual ~IRaftStorage()=default;
    virtual void PersistHardState(uint64_t currentTerm,int voteFor)=0;
    virtual void PersistEntries(const std::vector<LogEntry>& entries)=0;
    virtual void CompactTo(uint64_t snapshotIndex,uint64_t snapshotTerm)=0;

    virtual std::unique_ptr<ILog> OpenLog()=0;
    virtual std::unique_ptr<ISnapshotStore> OpenSnapshot()=0;
};