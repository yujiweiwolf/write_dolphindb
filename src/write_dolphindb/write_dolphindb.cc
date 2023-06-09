#include "write_dolphindb.h"

namespace co {

    DolphindbWriter::DolphindbWriter() {
        feed_queue_ = std::make_shared<StringQueue>();
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
        string feed_gateway_ = Config::Instance()->feed_gateway();
        LOG_INFO << "feed_gateway: " << feed_gateway_;
        co::MyFeedService feeder;
        if (!feed_gateway_.empty()) {
            feeder.set_queue(feed_queue_);
            feeder.Init(feed_gateway_);
            feeder.SubQTick("");
            feeder.Start();
        }

        std::string raw;
        int64_t type = 0;
        while (true) {
            if (!feed_queue_->Empty()) {
                type = feed_queue_->Pop(&raw);
                if (type != 0) {
                    switch (type) {
                        case kFBPrefixQTick: {
                            WriteQTick(raw);
                            break;
                        }
                        case kFBPrefixQOrder: {
                            WriteQOrder(raw);
                            break;
                        }
                        case kFBPrefixQKnock: {
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
