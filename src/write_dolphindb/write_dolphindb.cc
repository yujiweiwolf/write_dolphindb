#include "write_dolphindb.h"
#include <filesystem>
namespace fs = std::filesystem;

namespace co {

    DolphindbWriter::DolphindbWriter() {
    }

    DolphindbWriter::~DolphindbWriter() {
    }

    void DolphindbWriter::Init() {
        host_ = Config::Instance()->host();
        port_ = Config::Instance()->port();
        userId_ = Config::Instance()->userId();
        password_ = Config::Instance()->password();
        try {
            string initialScript = "";
            bool highAvailability = false;
            vector<string> highAvailabilitySites;
            int keepAliveTime = 30;
            bool reconnect = true;
            bool ret = conn.connect(host_, port_, userId_, password_, initialScript, highAvailability, highAvailabilitySites, keepAliveTime, reconnect);
            if (!ret) {
                LOG_ERROR << "Failed to connect dolphindb";
                return;
            } else {
                LOG_INFO << "Succeed to connect dolphindb";
            }
        } catch (exception &ex) {
            LOG_ERROR << "Failed to  connect  with error: " << ex.what();
            return;
        }
        int type = Config::Instance()->type();
        dbpath_ = Config::Instance()->dbpath();
        tradeknockname_ = Config::Instance()->tradeknockname();
        tickname_ = Config::Instance()->tickname();
        ordername_ = Config::Instance()->ordername();
        knockname_ = Config::Instance()->knockname();

        if (tickname_.length() > 0) {
            tick_writer_ = std::make_shared<TickWriter>();
            tick_writer_->SetDBConnection(&conn, dbpath_, tickname_);
        }
        if (ordername_.length() > 0) {
            order_writer_ = std::make_shared<OrderWriter>();
            order_writer_->SetDBConnection(&conn, dbpath_, ordername_);
        }
        if (knockname_.length() > 0) {
            knock_writer_ = std::make_shared<KnockWriter>();
            knock_writer_->SetDBConnection(&conn, dbpath_, knockname_);
        }
        if (tradeknockname_.length() > 0) {
            tradeknock_writer_ = std::make_shared<TradeKnockWriter>();
            tradeknock_writer_->SetDBConnection(&conn, dbpath_, tradeknockname_);
        }

        if (type == 1) {
            Run();
        } else if (type == 2) {
            string wal_file = Config::Instance()->wal_file();
            ReadWal(wal_file);
        }
    }

    void DolphindbWriter::ReadWal(const string& file) {
        static int tick_num = 0;
        static int order_num = 0;
        static int knock_num = 0;
        co::WALReader reader;
        reader.Open(file.c_str());
        while (true) {
            std::string raw;
            int64_t type = reader.Read(&raw);
            if (raw.empty()) {
                break;
            }
            switch (type) {
                case kFBPrefixQTick: {
                    tick_num++;
                    if (tick_num % 10000 == 0) {
                        // x::Sleep(100);
                        LOG_INFO << "tick num: " << tick_num;
                    }
                    WriteQTick(raw);
                    break;
                }
                case kFBPrefixQOrder: {
                    order_num++;
                    if (order_num % 10000 == 0) {
                        // x::Sleep(100);
                        LOG_INFO << "order num: " << order_num;
                    }
                    WriteQOrder(raw);
                    break;
                }
                case kFBPrefixQKnock: {
                    knock_num++;
                    if (knock_num % 10000 == 0) {
                        // x::Sleep(100);
                        LOG_INFO << "knock num: " << knock_num;
                    }
                    WriteQKnock(raw);
                    break;
                }
                default: {
                    break;
                }
            }
        }
        LOG_INFO << "read file: " << file << ", tick_num: " << tick_num
                << ", order_num: " << order_num
                << ", knock_num: " << knock_num;
    }

    void DolphindbWriter::Run() {
        x::MMapReader feeder_reader_;
        string mmap = Config::Instance()->mmap();
        std::vector<std::string> all_directors;
        x::Split(&all_directors, mmap, ";", true);
        for (auto& it: all_directors) {
            std::vector<std::string> sub_directors;
            if (!fs::exists(it) || !fs::is_directory(it)) {
                continue;
            }
            for (auto& entry : fs::directory_iterator(it)) {
                if (fs::is_directory(entry)) {
                    sub_directors.push_back(entry.path().string());
                }
            }
            sort(sub_directors.begin(), sub_directors.end(), [](string a, string b) {return a > b; });
//            for (auto& it : sub_directors) {
//                LOG_INFO << it;
//            }
            if (!sub_directors.empty()) {
                string file = sub_directors.front();
                LOG_INFO << "mmap open file: " << file;
                feeder_reader_.Open(file, "data");
                feeder_reader_.Open(file, "meta");
            }
        }
        void* data = nullptr;
        while (true) {
            int32_t type = feeder_reader_.Read(&data);
            if (type == kMemTypeQContract) {
                if (tick_writer_) {
                    MemQContract *contract = (MemQContract *) data;
                    tick_writer_->HandleContract(contract);
                }
            } else if (type == kMemTypeQTick) {
                if (tick_writer_) {
                    MemQTick *tick = (MemQTick *) data;
                    tick_writer_->HandleTick(tick);
                }
            } else if (type == kMemTypeQOrder) {
                MemQOrder *order = (MemQOrder *) data;
                if (order_writer_) {
                    order_writer_->WriteDate(order);
                }
            } else if (type == kMemTypeQKnock) {
                MemQKnock *knock = (MemQKnock *) data;
                if (knock_writer_) {
                    knock_writer_->WriteDate(knock);
                }
            }
        }
    }

    void DolphindbWriter::WriteQTick(std::string& raw) {
        if (tick_writer_) {
            tick_writer_->WriteDate(raw);
        }
    }

    void DolphindbWriter::WriteQOrder(std::string& raw) {
        if (order_writer_) {
            order_writer_->WriteDate(raw);
        }
    }

    void DolphindbWriter::WriteQKnock(std::string& raw) {
        if (knock_writer_) {
            knock_writer_->WriteDate(raw);
        }
    }

    void DolphindbWriter::WriteTradeKnock(std::string& raw) {
        if (tradeknock_writer_) {
            tradeknock_writer_->WriteDate(raw);
        }
    }
}
