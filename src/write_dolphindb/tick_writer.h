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
                    script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`date`code,sortColumns=`code`date`time,keepDuplicates=FIRST);";
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

        void HandleContract(MemQContract* data) {
            all_contract_.insert(std::make_pair(data->code, *data));
        }

        void HandleTick(MemQTick* data) {
            auto it = all_contract_.find(data->code);
            if (it == all_contract_.end()) {
                return;
            }
            if (write_step_ == 0) {
                write_step_++;
                string script;
                script += "existsTable(\"" + dbpath_ + "\", `" + tablename_ + ");";
                LOG_INFO << script;
                TableSP result = conn_->run(script);
                LOG_INFO << dbpath_ << ", " << tablename_ << ", exist result: " << result->getString();
                if (result->getString() == "0") {
                    string script;
                    TableSP table = createTable(data, &it->second);
                    conn_->upload("mt", table);
                    script += "login(`" + userId_ + ",`" + password_ + ");";
                    script += "dbPath = \"" + dbpath_ + "\";";
                    script += "db1 = database("", VALUE, 2023.01.01..2023.12.31);";
                    script += "db2 = database(\"\", HASH,[STRING,10]);";
                    script += "tableName = `" + tablename_ + ";";
                    script += "db = database(dbPath,COMPO,[db1,db2],engine=\"TSDB\");";
                    script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`date`code,sortColumns=`code`date`time,keepDuplicates=FIRST);";
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
                InsertDate(data, &it->second);
            }
        }

    private:
        TableSP createTable(std::string& raw) {
            vector<string> colNames = { "code","date","time","src","state",
                                        "bp0","bp1","bp2","bp3","bp4","bp5","bp6","bp7","bp8","bp9",
                                        "bv0","bv1","bv2","bv3","bv4","bv5","bv6","bv7","bv8","bv9",
                                        "ap0","ap1","ap2","ap3","ap4","ap5","ap6","ap7","ap8","ap9",
                                        "av0","av1","av2","av3","av4","av5","av6","av7","av8","av9",
                                        "new_price","new_volume","new_amount","sum_volume","sum_amount",
                                        "new_bid_volume","new_bid_amount","new_ask_volume","new_ask_amount",
                                        "open","close","settle","open_interest",
                                        "dtype","market","name","pre_close","upper_limit","lower_limit","pre_settle",
                                        "pre_open_interest","multiple","price_step",
                                        "create_date","list_date","expire_date","start_settle_date","end_settle_date","exercise_date",
                                        "exercise_price","cp_flag","underlying_code","trading_date"};

            vector<DATA_TYPE> colTypes = {DT_SYMBOL,DT_DATE,DT_TIME,DT_CHAR,DT_CHAR,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,
                                          DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,
                                          DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_LONG,
                                          DT_CHAR,DT_CHAR,DT_STRING,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_DOUBLE,DT_DOUBLE,
                                          DT_INT,DT_INT,DT_INT,DT_INT,DT_INT,DT_INT,
                                          DT_DOUBLE,DT_CHAR,DT_STRING,DT_LONG};

            int colNum = colNames.size(), rowNum = 1;
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
                columnVecs[index++]->set(i, Util::createChar(q->status()));
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
                columnVecs[index++]->set(i, Util::createDouble(q->new_price()));
                columnVecs[index++]->set(i, Util::createLong(q->new_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->new_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->sum_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->sum_amount()));

                columnVecs[index++]->set(i, Util::createLong(q->new_bid_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->new_bid_amount()));
                columnVecs[index++]->set(i, Util::createLong(q->new_ask_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->new_ask_amount()));

                columnVecs[index++]->set(i, Util::createDouble(q->open()));
                columnVecs[index++]->set(i, Util::createDouble(q->close()));
                columnVecs[index++]->set(i, Util::createDouble(q->settle()));
                columnVecs[index++]->set(i, Util::createLong(q->open_interest()));

                columnVecs[index++]->set(i, Util::createChar(q->dtype()));
                columnVecs[index++]->set(i, Util::createChar(q->market()));
                columnVecs[index++]->set(i, Util::createString(name));
                columnVecs[index++]->set(i, Util::createDouble(q->pre_close()));
                columnVecs[index++]->set(i, Util::createDouble(q->upper_limit()));
                columnVecs[index++]->set(i, Util::createDouble(q->lower_limit()));
                columnVecs[index++]->set(i, Util::createDouble(q->pre_settle()));

                columnVecs[index++]->set(i, Util::createLong(q->pre_open_interest()));
                columnVecs[index++]->set(i, Util::createDouble(q->multiple()));
                columnVecs[index++]->set(i, Util::createDouble(q->price_step()));

                columnVecs[index++]->set(i, Util::createInt(q->create_date()));
                columnVecs[index++]->set(i, Util::createInt(q->list_date()));
                columnVecs[index++]->set(i, Util::createInt(q->expire_date()));
                columnVecs[index++]->set(i, Util::createInt(q->start_settle_date()));
                columnVecs[index++]->set(i, Util::createInt(q->end_settle_date()));
                columnVecs[index++]->set(i, Util::createInt(q->exercise_date()));

                columnVecs[index++]->set(i, Util::createDouble(q->exercise_price()));
                columnVecs[index++]->set(i, Util::createChar(q->cp_flag()));
                columnVecs[index++]->set(i, Util::createString(underlying_code));
                columnVecs[index++]->set(i, Util::createLong(q->date()));
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
                    , Util::createChar(q->status())

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

                    ,Util::createDouble(q->new_price())
                    ,Util::createLong(q->new_volume())
                    ,Util::createDouble(q->new_amount())
                    ,Util::createLong(q->sum_volume())
                    ,Util::createDouble(q->sum_amount())

                    ,Util::createLong(q->new_bid_volume())
                    ,Util::createDouble(q->new_bid_amount())
                    ,Util::createLong(q->new_ask_volume())
                    ,Util::createDouble(q->new_ask_amount())

                    ,Util::createDouble(q->open())
                    ,Util::createDouble(q->close())
                    ,Util::createDouble(q->settle())
                    ,Util::createLong(q->open_interest())

                    ,Util::createChar(q->dtype())
                    ,Util::createChar(q->market())
                    ,Util::createString(name)
                    ,Util::createDouble(q->pre_close())
                    ,Util::createDouble(q->upper_limit())
                    ,Util::createDouble(q->lower_limit())
                    ,Util::createDouble(q->pre_settle())

                    ,Util::createLong(q->pre_open_interest())
                    ,Util::createDouble(q->multiple())
                    ,Util::createDouble(q->price_step())

                    ,Util::createInt(q->create_date())
                    ,Util::createInt(q->list_date())
                    ,Util::createInt(q->expire_date())
                    ,Util::createInt(q->start_settle_date())
                    ,Util::createInt(q->end_settle_date())
                    ,Util::createInt(q->exercise_date())

                    ,Util::createDouble(q->exercise_price())
                    ,Util::createChar(q->cp_flag())
                    ,Util::createString(underlying_code)
                    ,Util::createLong(q->date())
            );
        }

        TableSP createTable(MemQTick* data, MemQContract* contract) {
            vector<string> colNames = { "code","date","time","src","state",
                                        "bp0","bp1","bp2","bp3","bp4","bp5","bp6","bp7","bp8","bp9",
                                        "bv0","bv1","bv2","bv3","bv4","bv5","bv6","bv7","bv8","bv9",
                                        "ap0","ap1","ap2","ap3","ap4","ap5","ap6","ap7","ap8","ap9",
                                        "av0","av1","av2","av3","av4","av5","av6","av7","av8","av9",
                                        "new_price","new_volume","new_amount","sum_volume","sum_amount",
                                        "new_bid_volume","new_bid_amount","new_ask_volume","new_ask_amount",
                                        "open","close","settle","open_interest",
                                        "dtype","market","name","pre_close","upper_limit","lower_limit","pre_settle",
                                        "pre_open_interest","multiple","price_step",
                                        "create_date","list_date","expire_date","start_settle_date","end_settle_date","exercise_date",
                                        "exercise_price","cp_flag","underlying_code","trading_date"};

            vector<DATA_TYPE> colTypes = {DT_SYMBOL,DT_DATE,DT_TIME,DT_CHAR,DT_CHAR,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,
                                          DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,
                                          DT_LONG,DT_DOUBLE,DT_LONG,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_LONG,
                                          DT_CHAR,DT_CHAR,DT_STRING,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_DOUBLE,DT_DOUBLE,
                                          DT_INT,DT_INT,DT_INT,DT_INT,DT_INT,DT_INT,
                                          DT_DOUBLE,DT_CHAR,DT_STRING,DT_LONG};

            int colNum = colNames.size(), rowNum = 1;
            ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
            vector<VectorSP> columnVecs;
            columnVecs.reserve(colNum);
            for (int i = 0;i < colNum;i++)
                columnVecs.emplace_back(table->getColumn(i));


            int64_t timestamp = data->timestamp;
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
                columnVecs[index++]->set(i, Util::createString(data->code));
                columnVecs[index++]->set(i, Util::createDate(year, month, day));
                columnVecs[index++]->set(i, Util::createTime(hour, min, second, micro_second));
                columnVecs[index++]->set(i, Util::createChar(data->src));
                columnVecs[index++]->set(i, Util::createChar(data->state));

                for (int j= 0; j < 10; j++) {
                    columnVecs[index++]->set(i, Util::createDouble(data->bp[j]));
                }
                for (int j= 0; j < 10; j++) {
                    columnVecs[index++]->set(i, Util::createLong(data->bv[j]));
                }
                for (int j= 0; j < 10; j++) {
                    columnVecs[index++]->set(i, Util::createDouble(data->ap[j]));
                }
                for (int j= 0; j < 10; j++) {
                    columnVecs[index++]->set(i, Util::createLong(data->av[j]));
                }
                columnVecs[index++]->set(i, Util::createDouble(data->new_price));
                columnVecs[index++]->set(i, Util::createLong(data->new_volume));
                columnVecs[index++]->set(i, Util::createDouble(data->new_amount));
                columnVecs[index++]->set(i, Util::createLong(data->sum_volume));
                columnVecs[index++]->set(i, Util::createDouble(data->sum_amount));

                columnVecs[index++]->set(i, Util::createLong(data->new_bid_volume));
                columnVecs[index++]->set(i, Util::createDouble(data->new_bid_amount));
                columnVecs[index++]->set(i, Util::createLong(data->new_ask_volume));
                columnVecs[index++]->set(i, Util::createDouble(data->new_ask_amount));

                columnVecs[index++]->set(i, Util::createDouble(data->open));
                columnVecs[index++]->set(i, Util::createDouble(data->close));
                columnVecs[index++]->set(i, Util::createDouble(data->settle));
                columnVecs[index++]->set(i, Util::createLong(data->open_interest));

                columnVecs[index++]->set(i, Util::createChar(contract->dtype));
                columnVecs[index++]->set(i, Util::createChar(contract->market));
                columnVecs[index++]->set(i, Util::createString(contract->name));
                columnVecs[index++]->set(i, Util::createDouble(contract->pre_close));
                columnVecs[index++]->set(i, Util::createDouble(contract->upper_limit));
                columnVecs[index++]->set(i, Util::createDouble(contract->lower_limit));
                columnVecs[index++]->set(i, Util::createDouble(contract->pre_settle));

                columnVecs[index++]->set(i, Util::createLong(contract->pre_open_interest));
                columnVecs[index++]->set(i, Util::createDouble(contract->multiple));
                columnVecs[index++]->set(i, Util::createDouble(contract->price_step));

                columnVecs[index++]->set(i, Util::createInt(contract->create_date));
                columnVecs[index++]->set(i, Util::createInt(contract->list_date));
                columnVecs[index++]->set(i, Util::createInt(contract->expire_date));
                columnVecs[index++]->set(i, Util::createInt(contract->start_settle_date));
                columnVecs[index++]->set(i, Util::createInt(contract->end_settle_date));
                columnVecs[index++]->set(i, Util::createInt(contract->exercise_date));

                columnVecs[index++]->set(i, Util::createDouble(contract->exercise_price));
                columnVecs[index++]->set(i, Util::createChar(contract->cp_flag));
                columnVecs[index++]->set(i, Util::createString(contract->underlying_code));
                columnVecs[index++]->set(i, Util::createLong(contract->date));
            }
            return table;
        }

        void InsertDate(MemQTick* data, MemQContract* contract) {
            int64_t timestamp = data->timestamp;
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

            btw_->insert(dbpath_, tablename_
                    , Util::createString(data->code)
                    , Util::createDate(year, month, day)
                    , Util::createTime(hour, min, second, micro_second)
                    , Util::createChar(data->src)
                    , Util::createChar(data->state)

                    , Util::createDouble(data->bp[0])
                    , Util::createDouble(data->bp[1])
                    , Util::createDouble(data->bp[2])
                    , Util::createDouble(data->bp[3])
                    , Util::createDouble(data->bp[4])
                    , Util::createDouble(data->bp[5])
                    , Util::createDouble(data->bp[6])
                    , Util::createDouble(data->bp[7])
                    , Util::createDouble(data->bp[8])
                    , Util::createDouble(data->bp[9])
                    , Util::createLong(data->bv[0])
                    , Util::createLong(data->bv[1])
                    , Util::createLong(data->bv[2])
                    , Util::createLong(data->bv[3])
                    , Util::createLong(data->bv[4])
                    , Util::createLong(data->bv[5])
                    , Util::createLong(data->bv[6])
                    , Util::createLong(data->bv[7])
                    , Util::createLong(data->bv[8])
                    , Util::createLong(data->bv[9])

                    , Util::createDouble(data->ap[0])
                    , Util::createDouble(data->ap[1])
                    , Util::createDouble(data->ap[2])
                    , Util::createDouble(data->ap[3])
                    , Util::createDouble(data->ap[4])
                    , Util::createDouble(data->ap[5])
                    , Util::createDouble(data->ap[6])
                    , Util::createDouble(data->ap[7])
                    , Util::createDouble(data->ap[8])
                    , Util::createDouble(data->ap[9])
                    , Util::createLong(data->av[0])
                    , Util::createLong(data->av[1])
                    , Util::createLong(data->av[2])
                    , Util::createLong(data->av[3])
                    , Util::createLong(data->av[4])
                    , Util::createLong(data->av[5])
                    , Util::createLong(data->av[6])
                    , Util::createLong(data->av[7])
                    , Util::createLong(data->av[8])
                    , Util::createLong(data->av[9])

                    ,Util::createDouble(data->new_price)
                    ,Util::createLong(data->new_volume)
                    ,Util::createDouble(data->new_amount)
                    ,Util::createLong(data->sum_volume)
                    ,Util::createDouble(data->sum_amount)

                    ,Util::createLong(data->new_bid_volume)
                    ,Util::createDouble(data->new_bid_amount)
                    ,Util::createLong(data->new_ask_volume)
                    ,Util::createDouble(data->new_ask_amount)

                    ,Util::createDouble(data->open)
                    ,Util::createDouble(data->close)
                    ,Util::createDouble(data->settle)
                    ,Util::createLong(data->open_interest)

                    ,Util::createChar(contract->dtype)
                    ,Util::createChar(contract->market)
                    ,Util::createString(contract->name)
                    ,Util::createDouble(contract->pre_close)
                    ,Util::createDouble(contract->upper_limit)
                    ,Util::createDouble(contract->lower_limit)
                    ,Util::createDouble(contract->pre_settle)

                    ,Util::createLong(contract->pre_open_interest)
                    ,Util::createDouble(contract->multiple)
                    ,Util::createDouble(contract->price_step)

                    ,Util::createInt(contract->create_date)
                    ,Util::createInt(contract->list_date)
                    ,Util::createInt(contract->expire_date)
                    ,Util::createInt(contract->start_settle_date)
                    ,Util::createInt(contract->end_settle_date)
                    ,Util::createInt(contract->exercise_date)

                    ,Util::createDouble(contract->exercise_price)
                    ,Util::createChar(contract->cp_flag)
                    ,Util::createString(contract->underlying_code)
                    ,Util::createLong(contract->date)
            );
        }
    private:
        unordered_map<string, MemQContract> all_contract_;
    };
}
