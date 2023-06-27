#pragma once
#include "base_writer.h"

namespace co {
    class TradeKnockWriter : public BaseWriter {
    public:
        TradeKnockWriter() = default;
        virtual ~TradeKnockWriter() = default;

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
                    script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`date`code,sortColumns=`code`bid_order_no`ask_order_no`date,keepDuplicates=FIRST);";
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
            vector<string> colNames = { "code","trade_date","trade_time","id","trade_type","fund_id","username","inner_match_no","match_no",
                                        "market","name","order_no","batch_no","bs_flag","oc_flag","match_type",
                                        "match_volume","match_price","match_amount","error","recv_time"};
            vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_TIME,DT_STRING,DT_LONG,DT_STRING,DT_STRING,DT_STRING,DT_STRING,
                                          DT_LONG,DT_STRING,DT_STRING,DT_STRING,DT_LONG,DT_LONG,DT_LONG,
                                          DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_STRING,DT_LONG};
            int colNum = 21, rowNum = 1;
            ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 1);
            vector<VectorSP> columnVecs;
            columnVecs.reserve(colNum);
            for (int i = 0;i < colNum;i++)
                columnVecs.emplace_back(table->getColumn(i));

            auto q = flatbuffers::GetRoot<co::fbs::TradeKnock>(raw.data());
            string id = q->id() ? q->id()->str() : "";
            string fund_id = q->fund_id() ? q->fund_id()->str() : "";
            string username = q->username() ? q->username()->str() : "";
            string inner_match_no = q->inner_match_no() ? q->inner_match_no()->str() : "";
            string match_no = q->match_no() ? q->match_no()->str() : "";
            string code = q->code() ? q->code()->str() : "";
            string name = q->name() ? q->name()->str() : "";
            string order_no = q->order_no() ? q->order_no()->str() : "";
            string batch_no = q->batch_no() ? q->batch_no()->str() : "";
            string error = q->error() ? q->error()->str() : "";
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
                columnVecs[index++]->set(i, Util::createString(id));
                columnVecs[index++]->set(i, Util::createLong(q->trade_type()));
                columnVecs[index++]->set(i, Util::createString(fund_id));
                columnVecs[index++]->set(i, Util::createString(username));
                columnVecs[index++]->set(i, Util::createString(inner_match_no));
                columnVecs[index++]->set(i, Util::createString(match_no));

                columnVecs[index++]->set(i, Util::createLong(q->market()));
                columnVecs[index++]->set(i, Util::createString(name));
                columnVecs[index++]->set(i, Util::createString(order_no));
                columnVecs[index++]->set(i, Util::createString(batch_no));
                columnVecs[index++]->set(i, Util::createLong(q->bs_flag()));
                columnVecs[index++]->set(i, Util::createLong(q->oc_flag()));
                columnVecs[index++]->set(i, Util::createLong(q->match_type()));

                columnVecs[index++]->set(i, Util::createLong(q->match_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->match_price()));
                columnVecs[index++]->set(i, Util::createDouble(q->match_amount()));
                columnVecs[index++]->set(i, Util::createString(error));
                columnVecs[index++]->set(i, Util::createLong(q->recv_time()));
            }
            return table;
        }

        void InsertDate(std::string& raw) {
            auto q = flatbuffers::GetRoot<co::fbs::QKnock>(raw.data());
            string code = q->code() ? q->code()->str() : "";
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

            btw_->insert(dbpath_, tablename_
                    , Util::createString(code)
                    , Util::createDate(year, month, day)
                    , Util::createTime(hour, min, second, micro_second)
                    , Util::createLong(q->match_no())
                    , Util::createChar(q->bs_flag())
                    , Util::createLong(q->bid_order_no())
                    , Util::createLong(q->ask_order_no())
                    , Util::createLong(q->match_price())
                    , Util::createLong(q->match_volume())
                    , Util::createDouble(q->match_amount())
                    , Util::createLong(q->recv_time())
            );
        }
    };
}
