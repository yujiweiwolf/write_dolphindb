#pragma once
#include "base_writer.h"

namespace co {
    class EtfIopvWriter : public BaseWriter {
    public:
        EtfIopvWriter() = default;
        virtual ~EtfIopvWriter() = default;

        void WriteDate(std::string& raw) {

        }

        void WriteDate(MemQEtfIopv* data) {
            if (write_step_ == 0) {
                write_step_++;
                bool exit_db_flag = false;
                {
                    string script;
                    script += "existsDatabase(\"" + dbpath_ + "\");";
                    TableSP result = conn_->run(script);
                    if (result->getString() == "0") {
                        LOG_INFO << "not exit Database: " << dbpath_;
                    } else {
                        LOG_INFO << "exit Database: " << dbpath_;
                        exit_db_flag = true;
                    }
                }
                string script;
                script += "existsTable(\"" + dbpath_ + "\", `" + tablename_ + ");";
                TableSP result = conn_->run(script);
                LOG_INFO << dbpath_ << ", " << tablename_ << ", exist result: " << result->getString();
                if (result->getString() == "0") {
                    string script;
                    TableSP table = createTable(data);
                    conn_->upload("mt", table);
                    script += "login(`" + userId_ + ",`" + password_ + ");";
                    script += "dbPath = \"" + dbpath_ + "\";";
                    if (!exit_db_flag) {
                        script += "db1 = database("", VALUE, 2023.01.01..2023.12.31);";
                        script += "db2 = database(\"\", HASH,[STRING,10]);";
                        script += "tableName = `" + tablename_ + ";";
                        script += "db = database(dbPath,COMPO,[db1,db2],engine=\"TSDB\");";
                    } else {
                        script += "tableName = `" + tablename_ + ";";
                        script += "db = database(dbPath);";
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
                InsertDate(data);
            }
        }

    private:

        TableSP createTable(std::string& raw) {
            ConstantSP table;
            return table;
        }

        void InsertDate(std::string& raw) {

        }

        TableSP createTable(MemQEtfIopv* data) {
            vector<string> colNames = { "code","date","time","unit_volume",
                                        "sum_create_volume", "sum_redeem_volume","new_price","bp1","ap1","pre_close_iopv",
                                        "new_iopv", "bp1_iopv","ap1_iopv","mid_iopv",
                                        "basket0","basket1","basket2","basket3","basket4","basket5","basket6","basket7","basket8","basket9",
                                        "bid_iopv0","bid_iopv1","bid_iopv2","bid_iopv3","bid_iopv4","bid_iopv5","bid_iopv6","bid_iopv7","bid_iopv8","bid_iopv9",
                                        "ask_iopv0","ask_iopv1","ask_iopv2","ask_iopv3","ask_iopv4","ask_iopv5","ask_iopv6","ask_iopv7","ask_iopv8","ask_iopv9",
                                        "non_must_limit_up_rate","non_must_limit_down_rate","non_must_suspension_rate","must_non_suspension_rate",
                                        "create_stock_amendment_rate","redeem_stock_amendment_rate","deviate_valid","deviate_rate"};

            vector<DATA_TYPE> colTypes = {DT_SYMBOL,DT_DATE,DT_TIME,DT_LONG,
                                          DT_LONG,DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_LONG,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,DT_DOUBLE,
                                          DT_DOUBLE,DT_DOUBLE,DT_LONG,DT_DOUBLE};

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
                columnVecs[index++]->set(i, Util::createLong(data->unit_volume));

                columnVecs[index++]->set(i, Util::createLong(data->sum_create_volume));
                columnVecs[index++]->set(i, Util::createLong(data->sum_redeem_volume));
                columnVecs[index++]->set(i, Util::createDouble(data->new_price));
                columnVecs[index++]->set(i, Util::createDouble(data->bp1));
                columnVecs[index++]->set(i, Util::createDouble(data->ap1));
                columnVecs[index++]->set(i, Util::createDouble(data->pre_close_iopv));

                columnVecs[index++]->set(i, Util::createDouble(data->new_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->bp1_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->ap1_iopv));
                columnVecs[index++]->set(i, Util::createDouble(data->mid_iopv));

                for (int j= 0; j < 10; j++) {
                    columnVecs[index++]->set(i, Util::createLong(data->basket[j]));
                }
                for (int j= 0; j < 10; j++) {
                    columnVecs[index++]->set(i, Util::createDouble(data->bid_iopv[j]));
                }
                for (int j= 0; j < 10; j++) {
                    columnVecs[index++]->set(i, Util::createDouble(data->ask_iopv[j]));
                }

                columnVecs[index++]->set(i, Util::createDouble(data->non_must_limit_up_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->non_must_limit_down_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->non_must_suspension_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->must_non_suspension_rate));

                columnVecs[index++]->set(i, Util::createDouble(data->create_stock_amendment_rate));
                columnVecs[index++]->set(i, Util::createDouble(data->redeem_stock_amendment_rate));
                columnVecs[index++]->set(i, Util::createLong(data->deviate_valid));
                columnVecs[index++]->set(i, Util::createDouble(data->deviate_rate));
            }
            return table;
        }

        void InsertDate(MemQEtfIopv* data) {
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
                    , Util::createLong(data->unit_volume)

                    , Util::createLong(data->sum_create_volume)
                    , Util::createLong(data->sum_redeem_volume)
                    , Util::createDouble(data->new_price)
                    , Util::createDouble(data->bp1)
                    , Util::createDouble(data->ap1)
                    , Util::createDouble(data->pre_close_iopv)

                    , Util::createDouble(data->new_iopv)
                    , Util::createDouble(data->bp1_iopv)
                    , Util::createDouble(data->ap1_iopv)
                    , Util::createDouble(data->mid_iopv)

                    , Util::createLong(data->basket[0])
                    , Util::createLong(data->basket[1])
                    , Util::createLong(data->basket[2])
                    , Util::createLong(data->basket[3])
                    , Util::createLong(data->basket[4])
                    , Util::createLong(data->basket[5])
                    , Util::createLong(data->basket[6])
                    , Util::createLong(data->basket[7])
                    , Util::createLong(data->basket[8])
                    , Util::createLong(data->basket[9])

                    , Util::createDouble(data->bid_iopv[0])
                    , Util::createDouble(data->bid_iopv[1])
                    , Util::createDouble(data->bid_iopv[2])
                    , Util::createDouble(data->bid_iopv[3])
                    , Util::createDouble(data->bid_iopv[4])
                    , Util::createDouble(data->bid_iopv[5])
                    , Util::createDouble(data->bid_iopv[6])
                    , Util::createDouble(data->bid_iopv[7])
                    , Util::createDouble(data->bid_iopv[8])
                    , Util::createDouble(data->bid_iopv[9])

                    , Util::createDouble(data->ask_iopv[0])
                    , Util::createDouble(data->ask_iopv[1])
                    , Util::createDouble(data->ask_iopv[2])
                    , Util::createDouble(data->ask_iopv[3])
                    , Util::createDouble(data->ask_iopv[4])
                    , Util::createDouble(data->ask_iopv[5])
                    , Util::createDouble(data->ask_iopv[6])
                    , Util::createDouble(data->ask_iopv[7])
                    , Util::createDouble(data->ask_iopv[8])
                    , Util::createDouble(data->ask_iopv[9])

                    ,Util::createDouble(data->non_must_limit_up_rate)
                    ,Util::createDouble(data->non_must_limit_down_rate)
                    ,Util::createDouble(data->non_must_suspension_rate)
                    ,Util::createDouble(data->must_non_suspension_rate)

                    ,Util::createDouble(data->create_stock_amendment_rate)
                    ,Util::createDouble(data->redeem_stock_amendment_rate)
                    ,Util::createLong(data->deviate_valid)
                    ,Util::createDouble(data->deviate_rate)
            );
        }
    };
}
