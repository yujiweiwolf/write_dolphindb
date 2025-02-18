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
        etfiopvname_ = Config::Instance()->etfiopvname();

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
        if (etfiopvname_.length() > 0) {
            etfiopv_writer_ = std::make_shared<EtfIopvWriter>();
            etfiopv_writer_->SetDBConnection(&conn, dbpath_, etfiopvname_);
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
        string key = std::to_string(Config::Instance()->sub_date());
        if (!fs::exists(dir)) {
            LOG_ERROR << dir << "not exit";
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
        int64_t sub_date = Config::Instance()->sub_date();
        string mmap_file = Config::Instance()->mmap();
        std::vector<std::string> all_file;
        x::Split(&all_file, mmap_file, ";");
        for (auto& file : all_file) {
            if (fs::exists(file)) {
                LOG_INFO << "mmap open file: " << file;
                x::MMapReader feeder_reader_;
                feeder_reader_.Open(file, "data");
                feeder_reader_.Open(file, "meta");
                const void* data = nullptr;
                int tick_num = 0;
                int order_num = 0;
                int knock_num = 0;
                int iopv_num = 0;
                int64_t start_timestamp = 0;
                while (true) {
                    int32_t type = feeder_reader_.Next(&data);
                    // LOG_INFO << type;
                    if (type == kMemTypeQTickHead) {
                        if (tick_writer_) {
                            MemQTickHead *contract = (MemQTickHead*) data;
                            // LOG_INFO << ToString(contract);
                            int64_t date = 0;
                            if (contract->date != 0) {
                                date = contract->date;
                            } else {
                                date = contract->timestamp / 1000000000LL;
                            }
                            // LOG_INFO << date << ",  " << sub_date;
                            if (date == sub_date) {
                                tick_writer_->HandleQTickHead(contract);
                                if (start_timestamp == 0) {
                                    start_timestamp = contract->timestamp;
                                }
                                // 时间超过了一天
                                if (start_timestamp > 0) {
                                    if (x::SubRawDateTime(contract->timestamp, start_timestamp) > 24 * 3600 * 1000) {
                                        LOG_INFO << "finish, timestamp: " << contract->timestamp << ", start_timestamp: " << start_timestamp;
                                        break;
                                    }
                                }
                            }
                        }
                    } else if (type == kMemTypeQTickBody) {
                        if (tick_writer_) {
                            MemQTickBody *tick = (MemQTickBody*) data;
                            // int64_t date = tick->timestamp / 1000000000LL;
                            if (start_timestamp > 0 && tick->timestamp >= start_timestamp) {
                                tick_writer_->HandleTick(tick);
                                tick_num++;
                                if (tick_num % 10000 == 0) {
                                    LOG_INFO << "tick num: " << tick_num << ", code: " << tick->code << ", timestamp: " << tick->timestamp;
                                }
                            }
                        }
                    } else if (type == kMemTypeQOrder) {
                        MemQOrder *order = (MemQOrder *) data;
                        int64_t date = order->timestamp / 1000000000LL;
                        if (order_writer_ && date == sub_date) {
                            order_num++;
                            order_writer_->WriteDate(order);
                        }
                    } else if (type == kMemTypeQKnock) {
                        MemQKnock *knock = (MemQKnock *) data;
                        int64_t date = knock->timestamp / 1000000000LL;
                        if (knock_writer_ && date == sub_date) {
                            knock_num++;
                            knock_writer_->WriteDate(knock);
                        }
                    } else if (type == kMemTypeQEtfIopvHead) {
                        MemQEtfIopvHead *head = (MemQEtfIopvHead *) data;
                        int64_t date = head->timestamp / 1000000000LL;
                        // LOG_INFO << ToString(head);
                        if (etfiopv_writer_ && date == sub_date) {
                            etfiopv_writer_->HandleQTickHead(head);
                        }
                    } else if (type == kMemTypeQEtfIopvBody) {
                        MemQEtfIopvBody *body = (MemQEtfIopvBody *) data;
                        int64_t date = body->timestamp / 1000000000LL;
                        // LOG_INFO << ToString(body);
                        if (etfiopv_writer_&& date == sub_date) {
                            etfiopv_writer_->WriteDate(body);
                            iopv_num++;
                        }
                    } else if (type == 0) {
                        LOG_INFO << "read over, tick_num: " << tick_num
                                << ", order_num: " << order_num
                                << ", knock_num: " << knock_num
                                << ", iopv_num: " << iopv_num;
                        break;
                    }
                }
            } else {
                LOG_ERROR << file << " not exit";
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
