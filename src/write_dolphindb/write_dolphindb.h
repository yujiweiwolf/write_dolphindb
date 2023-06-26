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


//using namespace std;
//using namespace co;
//namespace po = boost::program_options;
//using namespace dolphindb;

namespace co {
    using namespace std;
    using namespace dolphindb;
    class DolphindbWriter {
    public:
        DolphindbWriter();
        ~DolphindbWriter();
        void Init();

    protected:
        void Run();
        void ReadWal(const string& file);
        void WriteQTick(std::string& raw);
        void WriteQOrder(std::string& raw);
        void WriteQKnock(std::string& raw);
        void WriteTradeKnock(std::string& raw);

    private:
        DBConnection conn;
        string host_;
        int port_ = 8848;
        string userId_ = "admin";
        string password_ = "123456";

        string dbpath_ = "";
        string tradeknockname_ = "";
        string tickname_ = "";
        int write_tick_step_ = 0;
        shared_ptr<BatchTableWriter> tick_btw_;
        string ordername_ = "";
        string knockname_ = "";

        std::string feed_gateway_;
        std::shared_ptr<StringQueue> feed_queue_ = nullptr;
        std::vector<std::unique_ptr<x::ZMQ>> socks_;

        shared_ptr<BatchTableWriter> qtick_writer_;
        shared_ptr<BatchTableWriter> qorder_writer_;
        shared_ptr<BatchTableWriter> qknock_writer_;
    };
}
