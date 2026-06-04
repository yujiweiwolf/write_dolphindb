#pragma once
#include "base_writer.h"

namespace co {
    class EtfIopvWriter : public BaseWriter {
    public:
        EtfIopvWriter() = default;
        virtual ~EtfIopvWriter() = default;

        void WriteDate(std::string& raw) {
        }

        void HandleQTickHead(MemQEtfIopvHead* data) {
            all_head_.insert(std::make_pair(data->code, *data));
        }

        void WriteDate(MemQEtfIopvBody* data) {
            auto it = all_head_.find(data->code);
            if (it == all_head_.end()) {
                return;
            }
            if (write_step_ == 0) {
                write_step_++;
                string script;
                bool exit_database = false;
                script += "existsDatabase(\"" + dbpath_ + "\");";
                TableSP db_result = conn_->run(script);
                if (db_result->getString() == "1") {
                    exit_database = true;
                }
                LOG_INFO << script << ", exist result: " << db_result->getString();
                script = "";

                script += "existsTable(\"" + dbpath_ + "\", `" + tablename_ + ");";
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
                    if (exit_database) {
                        script += "db = database(dbPath);";
                    } else {
                        script += "db = database(dbPath,COMPO,[db1,db2],engine=\"TSDB\");";
                    }
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
            ConstantSP table;
            return table;
        }

        void InsertDate(std::string& raw) {

        }

        TableSP createTable(MemQEtfIopvBody* data, MemQEtfIopvHead* head) {
            vector<string> colNames = { "code","date","time","underlying_code","market","create_fee","redeem_fee","position_ratio","dividend_ratio","unit_volume","estimate_cash","pre_nav",
                                        "pre_estimate_cash","pre_cash_diff","pre_close_iopv","local_must_amount","cross_must_amount","etf_type",
                                        "sum_create_volume","sum_redeem_volume","new_price","bp1", "ap1","new_iopv","bp1_iopv","ap1_iopv","mid_iopv","bp1_shift_iopv","ap1_shift_iopv","fair_iopv",
                                        "deviate_valid","deviate_rate","local_new_amount","local_bp1_amount","local_ap1_amount","cross_new_amount","cross_bp1_amount","cross_ap1_amount",
                                        "non_must_limit_up_rate","non_must_limit_down_rate","non_must_suspension_rate","must_non_suspension_rate","create_stock_amendment_rate","redeem_stock_amendment_rate",
                                        "local_fair_amount","cross_fair_amount","fair_bp1_iopv","fair_ap1_iopv"};

            vector<DATA_TYPE> colTypes = {DT_SYMBOL,DT_DATE,DT_TIME,DT_SYMBOL,
                                          DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_LONG,
                                          DT_LONG,DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE};

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
                columnVecs[index++]->set(i, Util::createString(head->underlying_code));

                columnVecs[index++]->set(i, Util::createLong(head->market));
                columnVecs[index++]->set(i, Util::createDouble(head->create_fee));
                columnVecs[index++]->set(i, Util::createDouble(head->redeem_fee));
                columnVecs[index++]->set(i, Util::createDouble(head->position_ratio));
                columnVecs[index++]->set(i, Util::createDouble(head->dividend_ratio));
                columnVecs[index++]->set(i, Util::createLong(head->unit_volume));
                columnVecs[index++]->set(i, Util::createDouble(head->estimate_cash));
                columnVecs[index++]->set(i, Util::createDouble(head->pre_nav));
                columnVecs[index++]->set(i, Util::createDouble(head->pre_estimate_cash));
                columnVecs[index++]->set(i, Util::createDouble(head->pre_cash_diff));
                columnVecs[index++]->set(i, Util::createDouble(head->pre_close_iopv));
                columnVecs[index++]->set(i, Util::createDouble(head->local_must_amount));
                columnVecs[index++]->set(i, Util::createDouble(head->cross_must_amount));
                columnVecs[index++]->set(i, Util::createLong(head->etf_type));

                columnVecs[index++]->set(i, Util::createLong(data->sum_create_volume));
                columnVecs[index++]->set(i, Util::createLong(data->sum_redeem_volume));
                columnVecs[index++]->set(i, Util::createDouble(data->new_price));
                columnVecs[index++]->set(i, Util::createDouble(data->bp1));
                columnVecs[index++]->set(i, Util::createDouble(data->ap1));
                columnVecs[index++]->set(i, Util::createDouble(data->new_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->bp1_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->ap1_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->mid_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->bp1_shift_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->ap1_shift_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->fair_iopv));

                columnVecs[index++]->set(i, Util::createLong(data->deviate_valid));
                columnVecs[index++]->set(i, Util::createDouble(data->deviate_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->local_new_amount));
                columnVecs[index++]->set(i, Util::createDouble(data->local_bp1_amount));
                columnVecs[index++]->set(i, Util::createDouble(data->local_ap1_amount));
                columnVecs[index++]->set(i, Util::createDouble(data->cross_new_amount));
                columnVecs[index++]->set(i, Util::createDouble(data->cross_bp1_amount));
                columnVecs[index++]->set(i, Util::createDouble(data->cross_ap1_amount));

                columnVecs[index++]->set(i, Util::createDouble(data->non_must_limit_up_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->non_must_limit_down_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->non_must_suspension_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->must_non_suspension_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->create_stock_amendment_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->redeem_stock_amendment_rate));

                columnVecs[index++]->set(i, Util::createDouble(data->local_fair_amount));
                columnVecs[index++]->set(i, Util::createDouble(data->cross_fair_amount));
                columnVecs[index++]->set(i, Util::createDouble(data->fair_bp1_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->fair_ap1_iopv));
            }
            return table;
        }

        void InsertDate(MemQEtfIopvBody* data, MemQEtfIopvHead* head) {
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
                    , Util::createString(head->underlying_code)

                    , Util::createLong(head->market)
                    , Util::createDouble(head->create_fee)
                    , Util::createDouble(head->redeem_fee)
                    , Util::createDouble(head->position_ratio)
                    , Util::createDouble(head->dividend_ratio)
                    , Util::createLong(head->unit_volume)
                    , Util::createDouble(head->estimate_cash)
                    , Util::createDouble(head->pre_nav)
                    , Util::createDouble(head->pre_estimate_cash)
                    , Util::createDouble(head->pre_cash_diff)
                    , Util::createDouble(head->pre_close_iopv)
                    , Util::createDouble(head->local_must_amount)
                    , Util::createDouble(head->cross_must_amount)
                    , Util::createLong(head->etf_type)

                    , Util::createLong(data->sum_create_volume)
                    , Util::createLong(data->sum_redeem_volume)
                    , Util::createDouble(data->new_price)
                    , Util::createDouble(data->bp1)
                    , Util::createDouble(data->ap1)
                    , Util::createDouble(data->new_iopv)
                    , Util::createDouble(data->bp1_iopv)
                    , Util::createDouble(data->ap1_iopv)
                    , Util::createDouble(data->mid_iopv)
                    , Util::createDouble(data->bp1_shift_iopv)
                    , Util::createDouble(data->ap1_shift_iopv)
                    , Util::createDouble(data->fair_iopv)

                    ,Util::createLong(data->deviate_valid)
                    ,Util::createDouble(data->deviate_rate)
                    ,Util::createDouble(data->local_new_amount)
                    ,Util::createDouble(data->local_bp1_amount)
                    ,Util::createDouble(data->local_ap1_amount)
                    ,Util::createDouble(data->cross_new_amount)
                    ,Util::createDouble(data->cross_bp1_amount)
                    ,Util::createDouble(data->cross_ap1_amount)

                    ,Util::createDouble(data->non_must_limit_up_rate)
                    ,Util::createDouble(data->non_must_limit_down_rate)
                    ,Util::createDouble(data->non_must_suspension_rate)
                    ,Util::createDouble(data->must_non_suspension_rate)
                    ,Util::createDouble(data->create_stock_amendment_rate)
                    ,Util::createDouble(data->redeem_stock_amendment_rate)

                    ,Util::createDouble(data->local_fair_amount)
                    ,Util::createDouble(data->cross_fair_amount)
                    ,Util::createDouble(data->fair_bp1_iopv)
                    ,Util::createDouble(data->fair_ap1_iopv)
            );
        }
    private:
        unordered_map<string, MemQEtfIopvHead> all_head_;
    };
}
