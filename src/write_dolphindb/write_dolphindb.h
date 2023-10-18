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
#include "tick_writer.h"
#include "order_writer.h"
#include "knock_writer.h"
#include "tradeknock_writer.h"

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
        void ReceiveSocket();
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
        shared_ptr<TradeKnockWriter> tradeknock_writer_;

        string tickname_ = "";
        shared_ptr<TickWriter> tick_writer_;

        string ordername_ = "";
        shared_ptr<OrderWriter> order_writer_;

        string knockname_ = "";
        shared_ptr<KnockWriter> knock_writer_;

        std::shared_ptr<StringQueue> feed_queue_ = nullptr;
    };
}
