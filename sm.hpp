#pragma once
#include "types.hpp"

class IStateMachine{
public:
    virtual ~IStateMachine()=default;
    virtual Status Apply(uint64_t index,const std::string& command)=0;
    virtual std::string TakeSnapshot(uint64_t lastIncludeIndex, uint64_t lastIncludeTerm)=0;
    virtual void RestoreSnapshot(const std::string& blob)=0;
};
