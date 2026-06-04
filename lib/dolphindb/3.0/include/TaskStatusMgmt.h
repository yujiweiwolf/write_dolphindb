// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Constant.h"
#include "Concurrent.h"

#include <condition_variable>
#include <utility>

namespace dolphindb {

enum class TaskStatus {
    WAITING,
    FINISHED,
    ERRORED,
};

struct TaskResult {
    explicit TaskResult(TaskStatus s = TaskStatus::WAITING, const ConstantSP &c = Constant::void_, std::string msg = "")
        : stage(s), result(c), errMsg(std::move(msg)) {}
    TaskStatus stage;
    ConstantSP result;
    std::string errMsg;
};
class TaskStatusMgmt{
public:
    bool isFinished(int identity);
    ConstantSP getData(int identity);
    void setResult(int identity, TaskResult);
private:
    std::mutex mutex_;
    std::unordered_map<int, TaskResult> results;
};

} // namespace dolphindb
