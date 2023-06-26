// Copyright 2020 Fancapital Inc.  All rights reserved.
#include "./config.h"
#include <coral/coral.h>
#include <boost/filesystem.hpp>
#include "yaml-cpp/yaml.h"

namespace co {
    Config* Config::instance_ = 0;

    Config* Config::Instance() {
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            if (instance_ == 0) {
                instance_ = new Config();
                instance_->Init();
            }
        });
        return instance_;
    }
    void Config::Init() {
        auto getStr = [&](const YAML::Node& node, const std::string& name) {
            try {
                return node[name] && !node[name].IsNull() ? node[name].as<std::string>() : "";
            } catch (std::exception& e) {
                LOG_ERROR << "load configuration failed: name = " << name << ", error = " << e.what();
                throw std::runtime_error(e.what());
            }
        };
        auto getStrings = [&](std::vector<std::string>* ret, const YAML::Node& node, const std::string& name, bool drop_empty = false) {
            try {
                if (node[name] && !node[name].IsNull()) {
                    for (auto item : node[name]) {
                        std::string s = x::Trim(item.as<std::string>());
                        if (!drop_empty || !s.empty()) {
                            ret->emplace_back(s);
                        }
                    }
                }
            } catch (std::exception& e) {
                LOG_ERROR << "load configuration failed: name = " << name << ", error = " << e.what();
                throw std::runtime_error(e.what());
            }
        };
        auto getInt = [&](const YAML::Node& node, const std::string& name, const int64_t& default_value = 0) {
            try {
                return node[name] && !node[name].IsNull() ? node[name].as<int64_t>() : default_value;
            } catch (std::exception& e) {
                LOG_ERROR << "load configuration failed: name = " << name << ", error = " << e.what();
                throw std::runtime_error(e.what());
            }
        };
        auto filename = x::FindFile("config.yaml");
        YAML::Node root = YAML::LoadFile(filename);
        auto dolphindb = root["dolphindb"];
        host_ = getStr(dolphindb, "host");
        port_ = getInt(dolphindb, "port");
        userId_ = getStr(dolphindb, "userId");
        string __password_ = getStr(dolphindb, "password");
        password_ = DecodePassword(__password_);
        dbpath_ = getStr(dolphindb, "dbpath");
        tradeknockname_ = getStr(dolphindb, "tradeknockname");
        tickname_ = getStr(dolphindb, "tickname");
        ordername_ = getStr(dolphindb, "ordername");
        knockname_ = getStr(dolphindb, "knockname");

        auto address = root["address"];
        feed_gateway_ = getStr(address, "feed_gateway");
        trade_gateway_ = getStr(address, "trade_gateway");
        type_ = getInt(address, "type");
        wal_file_ = getStr(address, "wal_file");

        stringstream ss;
        ss << "+-------------------- configuration begin --------------------+" << endl;
        ss << endl;
        ss << "address:     " << endl
            << "  feed_gateway: " << feed_gateway_ << endl
            << "  trade_gateway: " << trade_gateway_ << endl
            << "  type: " << type_ << endl
            << "  wal_file: " << wal_file_ << endl
            << "dolphindb:      " << endl
            << "  host: " << host_ << endl
            << "  port: " << port_ << endl
            << "  userId: " << userId_ << endl
            << "  password: " << string(password_.length(), '*') << endl
            << "  dbpath: " << dbpath_ << endl
            << "  tradeknockname: " << tradeknockname_ << endl
            << "  tickname: " << tickname_ << endl
            << "  ordername: " << ordername_ << endl
            << "  knockname: " << knockname_ << endl;
        ss << "+-------------------- configuration end   --------------------+";
        __info << endl << ss.str();
    }
}  // namespace co
