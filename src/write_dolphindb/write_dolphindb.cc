#include "write_dolphindb.h"
#include <filesystem>
namespace fs = std::filesystem;

namespace co {

    DolphindbWriter::DolphindbWriter() : feed_queue_(std::make_shared<StringQueue>()){
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

        dbpath_ = Config::Instance()->dbpath();
        tradeknockname_ = Config::Instance()->tradeknockname();
        tickname_ = Config::Instance()->tickname();
        ordername_ = Config::Instance()->ordername();
        knockname_ = Config::Instance()->knockname();
        etfiopvkname_ = Config::Instance()->etfiopvkname();

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
        if (etfiopvkname_.length() > 0) {
            etfiopv_writer_ = std::make_shared<EtfIopvWriter>();
            etfiopv_writer_->SetDBConnection(&conn, dbpath_, etfiopvkname_);
        }

        int type = Config::Instance()->type();
        if (type == 1) {
            ReadMMap();
        } else if (type == 2) {
            ReadWal();
        } else if (type == 3) {
            string feed_gateway = Config::Instance()->feed_gateway();;
            co::FeedService feeder;
            if (!feed_gateway.empty()) {
                feeder.set_queue(feed_queue_);
                feeder.Init(feed_gateway);
                feeder.set_disable_index(true);
                feeder.SubQTick("");
                feeder.SubQOrder("");
                feeder.SubQKnock("");
                feeder.Start();
                LOG_INFO << "start socket, feed_gateway: " << feed_gateway;
                ReceiveSocket();
            }
        }
    }

    void DolphindbWriter::ReceiveSocket() {
        std::string raw;
        int64_t type = 0;
        int tick_num = 0;
        int order_num = 0;
        int knock_num = 0;
        while (true) {
            if (!feed_queue_->Empty()) {
                type = feed_queue_->Pop(&raw);
                if (type != 0) {
                    switch (type) {
                        case kFBPrefixQTick: {
                            tick_num++;
                            if (tick_num % 10000 == 0) {
                                LOG_INFO << "tick num: " << tick_num;
                            }
                            WriteQTick(raw);
                            break;
                        }
                        case kFBPrefixQOrder: {
                            order_num++;
                            if (order_num % 10000 == 0) {
                                LOG_INFO << "order num: " << order_num;
                            }
                            WriteQOrder(raw);
                            break;
                        }
                        case kFBPrefixQKnock: {
                            knock_num++;
                            if (knock_num % 10000 == 0) {
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
            }
        }
    }

    void DolphindbWriter::ReadWal() {
        string dir = Config::Instance()->wal_file();
        string key = Config::Instance()->sub_date();
        if (!fs::exists(dir)) {
            return;
        }
        std::vector<std::string> files_;
        for (const fs::directory_entry& p : fs::directory_iterator(dir)) {
            std::string filename = p.path().filename().string();
            if (auto it = filename.find(key); it != filename.npos) {
                files_.push_back(fs::absolute(p.path()).string());
            }
        }
        std::sort(files_.begin(), files_.end());
        int tick_num = 0;
        int order_num = 0;
        int knock_num = 0;
        co::WALReader reader;
        for (auto& file : files_) {
            LOG_INFO << "read wal: " << file;
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
                            LOG_INFO << "tick num: " << tick_num;
                        }
                        WriteQTick(raw);
                        break;
                    }
                    case kFBPrefixQOrder: {
                        order_num++;
                        if (order_num % 10000 == 0) {
                            LOG_INFO << "order num: " << order_num;
                        }
                        WriteQOrder(raw);
                        break;
                    }
                    case kFBPrefixQKnock: {
                        knock_num++;
                        if (knock_num % 10000 == 0) {
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

    }

    void DolphindbWriter::ReadMMap() {
        x::MMapReader feeder_reader_;
        string mmap = Config::Instance()->mmap();
        std::vector<std::string> all_directors;
        x::Split(&all_directors, mmap, ";", true);
        for (auto& it: all_directors) {
            std::vector<std::string> sub_directors;
            if (!fs::exists(it) || !fs::is_directory(it)) {
                LOG_ERROR << "dir not exit, " << it;
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
        const void* data = nullptr;
        int tick_num = 0;
        int order_num = 0;
        int knock_num = 0;
        while (true) {
            int32_t type = feeder_reader_.Next(&data);
            if (type == kMemTypeQContract) {
                if (tick_writer_) {
                    MemQContract *contract = (MemQContract *) data;
                    tick_writer_->HandleContract(contract);
                }
            } else if (type == kMemTypeQTick) {
                if (tick_writer_) {
                    MemQTick *tick = (MemQTick *) data;
                    tick_writer_->HandleTick(tick);
                    order_num++;
                    if (order_num % 10000 == 0) {
                        LOG_INFO << "tick num: " << order_num << ", code: " << tick->code << ", timestamp: " << tick->timestamp;
                    }
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
            } else if (type == kMemTypeQEtfIopv) {
                MemQEtfIopv *iopv = (MemQEtfIopv *) data;
                if (etfiopv_writer_) {
                    etfiopv_writer_->WriteDate(iopv);
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
