// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <utility>

#include "DolphinDB.h"
#include "TaskStatusMgmt.h"

namespace dolphindb {

class DBConnectionPoolImpl{
public:
    struct Task{
        explicit Task(std::string script_ = "", std::shared_ptr<std::condition_variable> finished_ = nullptr, int id = 0, const RpcParam &param_ = RpcParam())
                : script(std::move(script_)), finished(std::move(finished_)), identity(id), param(param_) {}
        Task(std::string function, const std::vector<ConstantSP>& args, std::shared_ptr<std::condition_variable> finished_ = nullptr, int id = 0, const RpcParam &param_ = RpcParam())
            : script(std::move(function)), arguments(args), finished(std::move(finished_)), identity(id), param(param_), isFunc(true) {}
        std::string script;
        std::vector<ConstantSP> arguments;
        std::shared_ptr<std::condition_variable> finished;
        int identity;
        RpcParam param;
        bool isFunc = false;
    };
    
    DBConnectionPoolImpl(const std::string& hostName, int port, int threadNum = 10, const std::string& userId = "", const std::string& password = "",
        bool loadBalance = true, bool highAvailability = true, bool compress = false, bool reConnect=false, bool python = false);
    
    ~DBConnectionPoolImpl(){
        shutDown();
        Task emptyTask;
        for (size_t i = 0; i < workers_.size(); i++)
            queue_->push(emptyTask);
        for (auto& work : workers_) {
            work->join();
        }
    }

    int run(std::string script, int id,
        std::shared_ptr<std::condition_variable> finished = nullptr, const RpcParam &param = RpcParam())
    {
        taskStatus_.setResult(id, TaskResult());
        queue_->push(Task(std::move(script), std::move(finished), id, param));
        return id;
    }

    int run(std::string functionName, const std::vector<ConstantSP>& args,
        int id, std::shared_ptr<std::condition_variable> finished = nullptr, const RpcParam &param = RpcParam())
    {
        taskStatus_.setResult(id, TaskResult());
        queue_->push(Task(std::move(functionName), args, std::move(finished), id, param));
        return id;
    }

    bool isFinished(int identity){
        return taskStatus_.isFinished(identity);
    }

    ConstantSP getData(int identity){
        return taskStatus_.getData(identity);
    }

    void shutDown(){
        shutDownFlag_.store(true);
        latch_->wait();
    }

    bool isShutDown(){
        return shutDownFlag_.load();
    }

    int getConnectionCount(){
        return static_cast<int>(workers_.size());
    }
private:
    std::atomic<bool> shutDownFlag_;
    CountDownLatchSP latch_;
    std::vector<ThreadSP> workers_;
    std::shared_ptr<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt taskStatus_;
};

} // namespace dolphindb
