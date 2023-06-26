// Copyright 2020 Fancapital Inc.  All rights reserved.
#pragma once
#include <string>
#include "feeder/feeder.h"

using namespace std;

namespace co {
    class Config {
    public:
        static Config* Instance();

        inline string host() {
            return host_;
        }
        inline int port() {
            return port_;
        }
        inline string userId() {
            return userId_;
        }
        inline string password() {
            return password_;
        }
        inline string dbpath() {
            return dbpath_;
        }
        inline string tradeknockname() {
            return tradeknockname_;
        }
        inline string tickname() {
            return tickname_;
        }
        inline string ordername() {
            return ordername_;
        }
        inline string knockname() {
            return knockname_;
        }
        inline int type() {
            return type_;
        }
        inline string wal_file() {
            return wal_file_;
        }
        inline string feed_gateway() {
            return feed_gateway_;
        }
        inline string trade_gateway() {
            return trade_gateway_;
        }
    protected:
        Config() = default;
        ~Config() = default;
        Config(const Config&) = delete;
        const Config& operator=(const Config&) = delete;

        void Init();

    private:
        static Config* instance_;
        string host_;
        int port_;
        string userId_;
        string password_;
        string dbpath_;
        string tradeknockname_;
        string tickname_;
        string ordername_;
        string knockname_;

        string feed_gateway_;
        string trade_gateway_;
        int type_;
        string wal_file_;
    };
}  // namespace co
