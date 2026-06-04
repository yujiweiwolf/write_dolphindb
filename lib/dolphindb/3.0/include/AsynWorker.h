// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <utility>

#include "Concurrent.h"
#include "DBConnectionPoolImpl.h"

namespace dolphindb {
class DBConnection;
class AsynWorker: public Runnable {
public:
    using Task = DBConnectionPoolImpl::Task;
    AsynWorker(DBConnectionPoolImpl& pool, const CountDownLatchSP &latch, const std::shared_ptr<DBConnection>& conn,
               const std::shared_ptr<SynchronizedQueue<Task>>& queue, TaskStatusMgmt& status,
               std::string hostName, std::string userId , std::string password)
            : pool_(pool), latch_(latch), conn_(conn), queue_(queue),taskStatus_(status),
              hostName_(std::move(hostName)), userId_(std::move(userId)), password_(std::move(password)){}
protected:
    void run() override;

private:
    DBConnectionPoolImpl& pool_;
    CountDownLatchSP latch_;
    std::shared_ptr<DBConnection> conn_;
	std::shared_ptr<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt& taskStatus_;
    const std::string hostName_;
    const std::string userId_;
    const std::string password_;
};

} // namespace dolphindb
