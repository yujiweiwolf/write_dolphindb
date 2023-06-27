#pragma once
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <mutex>

#include "DolphinDB.h"
#include "BatchTableWriter.h"
#include "config.h"
#include <x/x.h>
#include "coral/wal_reader.h"
#include "feeder/feeder.h"

namespace co {
    using namespace std;
    using namespace dolphindb;
    class BaseWriter {
    public:
        BaseWriter() = default;
        virtual ~BaseWriter() = default;
        void SetDBConnection(DBConnection* conn, string dbpath, string tablename) {
            conn_ = conn;
            dbpath_ = dbpath;
            tablename_ = tablename;
            host_ = Config::Instance()->host();
            port_ = Config::Instance()->port();
            userId_ = Config::Instance()->userId();
            password_ = Config::Instance()->password();
        }

        virtual void WriteDate(std::string& raw) = 0;
    private:
        virtual TableSP createTable(std::string& raw) = 0;
        virtual void InsertDate(std::string& raw) = 0;

    protected:
        int write_step_ = 0;
        DBConnection* conn_;
        string host_;
        int port_;
        string userId_;
        string password_;
        string dbpath_;
        string tablename_;
        shared_ptr<BatchTableWriter> btw_;
    };
}
