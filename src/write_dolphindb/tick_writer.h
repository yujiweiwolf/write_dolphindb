#pragma once
#include "base_writer.h"

namespace co {
    class TickWriter : public BaseWriter {
    public:
        TickWriter() = default;
        virtual ~TickWriter() = default;

        void WriteDate(std::string& raw) {
            if (write_step_ == 0) {
                write_step_++;
                string script;
                script += "existsTable(\"" + dbpath_ + "\", `" + tablename_ + ");";
                LOG_INFO << script;
                TableSP result = conn_->run(script);
                LOG_INFO << dbpath_ << ", " << tablename_ << ", exist result: " << result->getString();
                if (result->getString() == "0") {
                    string script;
                    TableSP table = createTable(raw);
                    conn_->upload("mt", table);
                    script += "login(`" + userId_ + ",`" + password_ + ");";
                    script += "dbPath = \"" + dbpath_ + "\";";
                    script += "db1 = database("", VALUE, 2023.01.01..2023.12.31);";
                    script += "db2 = database(\"\", HASH,[STRING,10]);";
                    script += "tableName = `" + tablename_ + ";";
                    script += "db = database(dbPath,COMPO,[db1,db2],engine=\"TSDB\");";
                    script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`tick_date`code,sortColumns=`code`tick_date`tick_time,keepDuplicates=FIRST);";
                    script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
                    TableSP result = conn_->run(script);
                    return;
                }
            }

            if (write_step_ == 1) {
                write_step_++;
                btw_ = make_shared<BatchTableWriter>(host_, port_, userId_, password_, true);
                btw_->addTable(dbpath_, tablename_);
                LOG_INFO << "addTable, " << dbpath_ << ", " << tablename_;
            }

            if (write_step_ == 2) {
                InsertDate(raw);
            }
        }

    private:
        TableSP createTable(std::string& raw) {
            vector<string> colNames = { "code","tick_date","tick_time","src","dtype","name","market","pre_close","upper_limit","lower_limit",
                                        "bp0","bp1","bp2","bp3","bp4","bp5","bp6","bp7","bp8","bp9",
                                        "bv0","bv1","bv2","bv3","bv4","bv5","bv6","bv7","bv8","bv9",
                                        "ap0","ap1","ap2","ap3","ap4","ap5","ap6","ap7","ap8","ap9",
                                        "av0","av1","av2","av3","av4","av5","av6","av7","av8","av9",
                                        "status","new_price","new_volume","new_amount","sum_volume","sum_amount","open","high","low",
                                        "avg_bid_price","avg_ask_price","new_bid_volume","new_bid_amount","new_ask_volume","new_ask_amount",
                                        "open_interest","pre_settle","pre_open_interest","close","settle","multiple","price_step",
                                        "create_date","list_date","expire_date","start_settle_date","end_settle_date","exercise_date",
                                        "exercise_price","cp_flag","underlying_code","sum_bid_volume","sum_bid_amount",
                                        "sum_ask_volume","sum_ask_amount","bid_order_volume","bid_order_amount","bid_cancel_volume",
                                        "bid_cancel_amount","ask_order_volume","ask_order_amount","ask_cancel_volume","ask_cancel_amount",
                                        "new_knock_count","sum_knock_count","date","cursor"};

            vector<DATA_TYPE> colTypes = {DT_SYMBOL,DT_DATE,DT_TIME,DT_CHAR,DT_CHAR,DT_STRING,DT_CHAR,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,
                                          DT_CHAR,DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,
                                          DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_LONG,DT_DOUBLE,
                                          DT_INT,DT_INT,DT_INT,DT_INT,DT_INT,DT_INT,
                                          DT_DOUBLE,DT_CHAR,DT_STRING,DT_LONG,DT_DOUBLE,
                                          DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_LONG,
                                          DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_INT,DT_LONG};

            int colNum = 97, rowNum = 1;
            ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
            vector<VectorSP> columnVecs;
            columnVecs.reserve(colNum);
            for (int i = 0;i < colNum;i++)
                columnVecs.emplace_back(table->getColumn(i));

            auto q = flatbuffers::GetRoot<co::fbs::QTick>(raw.data());
            string code = q->code() ? q->code()->str() : "";
            string name = q->name() ? q->name()->str() : "";
            string underlying_code = q->underlying_code() ? q->underlying_code()->str() : "";
            int64_t timestamp = q->timestamp();
            int64_t date = timestamp / 1000000000LL;
            int year = date / 10000;
            date %= 10000;
            int month = date / 100;
            int day = date % 100;
            int64_t time = timestamp % 1000000000LL;
            int micro_second = time % 1000;
            time /= 1000;
            int hour = time / 10000;
            time %= 10000;
            int min = time / 100;
            int second = time % 100;
            for (int i = 0;i < rowNum; i++) {
                int index = 0;
                columnVecs[index++]->set(i, Util::createString(code));
                columnVecs[index++]->set(i, Util::createDate(year, month, day));
                columnVecs[index++]->set(i, Util::createTime(hour, min, second, micro_second));
                columnVecs[index++]->set(i, Util::createChar(q->src()));
                columnVecs[index++]->set(i, Util::createChar(q->dtype()));
                columnVecs[index++]->set(i, Util::createString(name));
                columnVecs[index++]->set(i, Util::createChar(q->market()));
                columnVecs[index++]->set(i, Util::createDouble(q->pre_close()));
                columnVecs[index++]->set(i, Util::createDouble(q->upper_limit()));
                columnVecs[index++]->set(i, Util::createDouble(q->lower_limit()));
                auto bps = q->bp();
                auto bvs = q->bv();
                auto aps = q->ap();
                auto avs = q->av();
                vector<double> all_bp(10), all_ap(10);
                vector<int64_t> all_bv(10), all_av(10);
                for (size_t j = 0; j < 10; ++j) {
                    all_bp[j] = 0;
                    all_ap[j] = 0;
                    all_bv[j] = 0;
                    all_av[j] = 0;
                }
                for (size_t j = 0; j < 10 && bps && bvs && j < bps->size() && j < bvs->size(); ++j) {
                    double bp = bps->Get(j);
                    int64_t bv = bvs->Get(j);
                    all_bp[j] = bp;
                    all_bv[j] = bv;
                    // LOG_INFO << "bp: " << bp << ", bv: " << bv;
                }
                for (size_t j = 0; j < 10 && aps && avs && j < aps->size() && j < avs->size(); ++j) {
                    double ap = aps->Get(j);
                    int64_t av = avs->Get(j);
                    all_ap[j] = ap;
                    all_av[j] = av;
                    // LOG_INFO << "ap: " << ap << ", av: " << av;
                }

                for (auto& bp : all_bp) {
                    columnVecs[index++]->set(i, Util::createDouble(bp));
                }
                for (auto& bv : all_bv) {
                    columnVecs[index++]->set(i, Util::createLong(bv));
                }
                for (auto& ap : all_ap) {
                    columnVecs[index++]->set(i, Util::createDouble(ap));
                }
                for (auto& av : all_av) {
                    columnVecs[index++]->set(i, Util::createLong(av));
                }

                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createChar(q->status()));
                columnVecs[index++]->set(i, Util::createDouble(q->new_price()));
                columnVecs[index++]->set(i, Util::createLong(q->new_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->new_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->sum_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->sum_amount()));
                columnVecs[index++]->set(i, Util::createDouble(q->open()));
                columnVecs[index++]->set(i, Util::createDouble(q->high()));
                columnVecs[index++]->set(i, Util::createDouble(q->low()));
                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createDouble(q->avg_bid_price()));
                columnVecs[index++]->set(i, Util::createDouble(q->avg_ask_price()));
                columnVecs[index++]->set(i, Util::createLong(q->new_bid_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->new_bid_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->new_ask_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->new_ask_amount()));
                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createLong(q->open_interest()));
                columnVecs[index++]->set(i, Util::createDouble(q->pre_settle()));
                columnVecs[index++]->set(i, Util::createLong(q->pre_open_interest()));
                columnVecs[index++]->set(i, Util::createDouble(q->close()));
                columnVecs[index++]->set(i, Util::createDouble(q->settle()));
                columnVecs[index++]->set(i, Util::createLong(q->multiple()));
                columnVecs[index++]->set(i, Util::createDouble(q->price_step()));
                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createInt(q->create_date()));
                columnVecs[index++]->set(i, Util::createInt(q->list_date()));
                columnVecs[index++]->set(i, Util::createInt(q->expire_date()));
                columnVecs[index++]->set(i, Util::createInt(q->start_settle_date()));
                columnVecs[index++]->set(i, Util::createInt(q->end_settle_date()));
                columnVecs[index++]->set(i, Util::createInt(q->exercise_date()));
                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createDouble(q->exercise_price()));
                columnVecs[index++]->set(i, Util::createChar(q->cp_flag()));
                columnVecs[index++]->set(i, Util::createString(underlying_code));
                columnVecs[index++]->set(i, Util::createLong(q->sum_bid_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->sum_bid_amount()));
                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createLong(q->sum_ask_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->sum_ask_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->bid_order_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->bid_order_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->bid_cancel_volume()));
                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createDouble(q->bid_cancel_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->ask_order_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->ask_order_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->ask_cancel_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->ask_cancel_amount()));
                ////////////////////////////////////////////
                columnVecs[index++]->set(i, Util::createLong(q->new_knock_count()));
                columnVecs[index++]->set(i, Util::createLong(q->sum_knock_count()));
                columnVecs[index++]->set(i, Util::createInt(q->date()));
                columnVecs[index++]->set(i, Util::createLong(q->cursor()));
            }
            return table;
        }

        void InsertDate(std::string& raw) {
            auto q = flatbuffers::GetRoot<co::fbs::QTick>(raw.data());
            string code = q->code() ? q->code()->str() : "";
            string name = q->name() ? q->name()->str() : "";
            string underlying_code = q->underlying_code() ? q->underlying_code()->str() : "";
            int64_t timestamp = q->timestamp();
            int64_t date = timestamp / 1000000000LL;
            int year = date / 10000;
            date %= 10000;
            int month = date / 100;
            int day = date % 100;
            int64_t time = timestamp % 1000000000LL;
            int micro_second = time % 1000;
            time /= 1000;
            int hour = time / 10000;
            time %= 10000;
            int min = time / 100;
            int second = time % 100;
            auto bps = q->bp();
            auto bvs = q->bv();
            auto aps = q->ap();
            auto avs = q->av();
            vector<double> all_bp(10), all_ap(10);
            vector<int64_t> all_bv(10), all_av(10);
            for (size_t j = 0; j < 10; ++j) {
                all_bp[j] = 0;
                all_ap[j] = 0;
                all_bv[j] = 0;
                all_av[j] = 0;
            }
            for (size_t j = 0; j < 10 && bps && bvs && j < bps->size() && j < bvs->size(); ++j) {
                double bp = bps->Get(j);
                int64_t bv = bvs->Get(j);
                all_bp[j] = bp;
                all_bv[j] = bv;
            }
            for (size_t j = 0; j < 10 && aps && avs && j < aps->size() && j < avs->size(); ++j) {
                double ap = aps->Get(j);
                int64_t av = avs->Get(j);
                all_ap[j] = ap;
                all_av[j] = av;
            }

            btw_->insert(dbpath_, tablename_
                    , Util::createString(code)
                    , Util::createDate(year, month, day)
                    , Util::createTime(hour, min, second, micro_second)
                    , Util::createChar(q->src())
                    , Util::createChar(q->dtype())
                    , Util::createString(name)
                    , Util::createChar(q->market())
                    , Util::createDouble(q->pre_close())
                    , Util::createDouble(q->upper_limit())
                    , Util::createDouble(q->lower_limit())

                    , Util::createDouble(all_bp[0])
                    , Util::createDouble(all_bp[1])
                    , Util::createDouble(all_bp[2])
                    , Util::createDouble(all_bp[3])
                    , Util::createDouble(all_bp[4])
                    , Util::createDouble(all_bp[5])
                    , Util::createDouble(all_bp[6])
                    , Util::createDouble(all_bp[7])
                    , Util::createDouble(all_bp[8])
                    , Util::createDouble(all_bp[9])
                    , Util::createLong(all_bv[0])
                    , Util::createLong(all_bv[1])
                    , Util::createLong(all_bv[2])
                    , Util::createLong(all_bv[3])
                    , Util::createLong(all_bv[4])
                    , Util::createLong(all_bv[5])
                    , Util::createLong(all_bv[6])
                    , Util::createLong(all_bv[7])
                    , Util::createLong(all_bv[8])
                    , Util::createLong(all_bv[9])

                    , Util::createDouble(all_ap[0])
                    , Util::createDouble(all_ap[1])
                    , Util::createDouble(all_ap[2])
                    , Util::createDouble(all_ap[3])
                    , Util::createDouble(all_ap[4])
                    , Util::createDouble(all_ap[5])
                    , Util::createDouble(all_ap[6])
                    , Util::createDouble(all_ap[7])
                    , Util::createDouble(all_ap[8])
                    , Util::createDouble(all_ap[9])
                    , Util::createLong(all_av[0])
                    , Util::createLong(all_av[1])
                    , Util::createLong(all_av[2])
                    , Util::createLong(all_av[3])
                    , Util::createLong(all_av[4])
                    , Util::createLong(all_av[5])
                    , Util::createLong(all_av[6])
                    , Util::createLong(all_av[7])
                    , Util::createLong(all_av[8])
                    , Util::createLong(all_av[9])

                    , Util::createChar(q->status())
                    , Util::createDouble(q->new_price())
                    , Util::createLong(q->new_volume())
                    , Util::createDouble(q->new_amount())
                    , Util::createLong(q->sum_volume())
                    , Util::createDouble(q->sum_amount())
                    , Util::createDouble(q->open())
                    , Util::createDouble(q->high())
                    , Util::createDouble(q->low())

                    , Util::createDouble(q->avg_bid_price())
                    , Util::createDouble(q->avg_ask_price())
                    , Util::createLong(q->new_bid_volume())
                    , Util::createDouble(q->new_bid_amount())
                    , Util::createLong(q->new_ask_volume())
                    , Util::createDouble(q->new_ask_amount())

                    , Util::createLong(q->open_interest())
                    , Util::createDouble(q->pre_settle())
                    , Util::createLong(q->pre_open_interest())
                    , Util::createDouble(q->close())
                    , Util::createDouble(q->settle())
                    , Util::createLong(q->multiple())
                    , Util::createDouble(q->price_step())

                    , Util::createInt(q->create_date())
                    , Util::createInt(q->list_date())
                    , Util::createInt(q->expire_date())
                    , Util::createInt(q->start_settle_date())
                    , Util::createInt(q->end_settle_date())
                    , Util::createInt(q->exercise_date())

                    , Util::createDouble(q->exercise_price())
                    , Util::createChar(q->cp_flag())
                    , Util::createString(underlying_code)
                    , Util::createLong(q->sum_bid_volume())
                    , Util::createDouble(q->sum_bid_amount())

                    , Util::createLong(q->sum_ask_volume())
                    , Util::createDouble(q->sum_ask_amount())
                    , Util::createLong(q->bid_order_volume())
                    , Util::createDouble(q->bid_order_amount())
                    , Util::createLong(q->bid_cancel_volume())

                    , Util::createDouble(q->bid_cancel_amount())
                    , Util::createLong(q->ask_order_volume())
                    , Util::createDouble(q->ask_order_amount())
                    , Util::createLong(q->ask_cancel_volume())
                    , Util::createDouble(q->ask_cancel_amount())

                    , Util::createLong(q->new_knock_count())
                    , Util::createLong(q->sum_knock_count())
                    , Util::createInt(q->date())
                    , Util::createLong(q->cursor())
            );
        }
    };
}
