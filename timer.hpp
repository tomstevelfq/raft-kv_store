#pragma once
#include<chrono>
#include<functional>

class ITimer{
public:
    virtual ~ITimer()=default;
    virtual void After(std::chrono::milliseconds ms,std::function<void()> cb)=0;
    virtual void every(std::chrono::milliseconds period,std::function<void()> cb)=0;
};