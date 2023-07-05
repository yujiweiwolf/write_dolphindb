#pragma once
#include "base_writer.h"

namespace co {
    class KnockWriter : public BaseWriter {
    public:
        KnockWriter() = default;
        virtual ~KnockWriter() = default;

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
                    script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`date`code,sortColumns=`code`bid_order_no`ask_order_no`date,keepDuplicates=FIRST,sortKeyMappingFunction=[hashBucket{,499}, hashBucket{,1}, hashBucket{, 1}]);";
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
            vector<string> colNames = { "code","date","time","match_no","bs_flag","bid_order_no","ask_order_no","match_price","match_volume","match_amount","recv_time"};
            vector<DATA_TYPE> colTypes = {DT_SYMBOL,DT_DATE,DT_TIME,DT_LONG,DT_CHAR,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_DOUBLE,DT_LONG};
            int colNum = 11, rowNum = 1;
            ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
            vector<VectorSP> columnVecs;
            columnVecs.reserve(colNum);
            for (int i = 0;i < colNum;i++)
                columnVecs.emplace_back(table->getColumn(i));

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
            for (int i = 0;i < rowNum; i++) {
                int index = 0;
                columnVecs[index++]->set(i, Util::createString(code));
                columnVecs[index++]->set(i, Util::createDate(year, month, day));
                columnVecs[index++]->set(i, Util::createTime(hour, min, second, micro_second));
                columnVecs[index++]->set(i, Util::createLong(q->match_no()));
                columnVecs[index++]->set(i, Util::createChar(q->bs_flag()));
                columnVecs[index++]->set(i, Util::createLong(q->bid_order_no()));
                columnVecs[index++]->set(i, Util::createLong(q->ask_order_no()));
                columnVecs[index++]->set(i, Util::createLong(q->match_price()));
                columnVecs[index++]->set(i, Util::createLong(q->match_volume()));
                columnVecs[index++]->set(i, Util::createDouble(q->match_amount()));
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
