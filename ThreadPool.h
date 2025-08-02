#pragma once
#include<vector>
#include<thread>
#include<queue>
#include<mutex>
#include<condition_variable>
#include<functional>
#include<future>
#include<atomic>

class ThreadPool{
    public:
    explicit ThreadPool(int numThreads);
    ~ThreadPool();
    template<class F,class... Args>
    auto submit(F&& f,Args&&... args)->std::future<decltype(f(args...))>;
    private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queueMutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
};

inline ThreadPool::ThreadPool(int numThreads):stop(false){
    for(int i=0;i<numThreads;i++){
        workers.emplace_back([this](){
            while(true){
                std::function<void()> task;
                //取任务
                {
                    std::unique_lock<std::mutex> lock(this->queueMutex);
                    this->condition.wait(lock,[this](){
                        return this->stop||!this->tasks.empty();
                    });
                    if(this->stop||this->tasks.empty()) return;

                    task=std::move(this->tasks.front());
                    this->tasks.pop();
                }
                task();
            }
        });
    }
}

inline ThreadPool::~ThreadPool(){
    stop=true;
    condition.notify_all();
    for(auto& thread:workers){
        if(thread.joinable()){
            thread.join();
        }
    }
}

template<class F,class... Args>
auto ThreadPool::submit(F&& f,Args&&... args)->std::future<decltype(f(args...))> {
    using return_type=decltype(f(args...));
    auto task=std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f),std::forward<Args>(args)...)
    );
    std::future<return_type> res=task->get_future();
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        tasks.emplace([task](){(*task)();});
    }

    condition.notify_one();
    return res;
}