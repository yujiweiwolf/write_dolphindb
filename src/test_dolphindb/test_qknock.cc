#include "DolphinDB.h"
#include "Util.h"
#include <iostream>
#include <sstream>
#include <string>
#include <sys/time.h>
#include "BatchTableWriter.h"

#include <iostream>
#include <boost/program_options.hpp>
#include <x/x.h>
#include "coral/wal_reader.h"
#include "feeder/feeder.h"

using namespace std;
using namespace co;
namespace po = boost::program_options;
using namespace dolphindb;

std::vector<string> all_out;
std::vector<co::fbs::QOrderT> all_order;
std::vector<co::fbs::QKnockT> all_knock;

TableSP createQKnockTable(std::string& raw) {
    vector<string> colNames = { "code","date","time","match_no","bs_flag","bid_order_no","ask_order_no","match_price","match_volume","match_amount","recv_time"};
    vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_TIME,DT_LONG,DT_CHAR,DT_LONG,DT_LONG,DT_LONG,DT_LONG,DT_DOUBLE,DT_LONG};
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

void GetData(const string& file) {
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
                break;
            }
            case kFBPrefixQOrder: {
                break;
            }
            case kFBPrefixQKnock: {
                all_out.push_back(raw);
                break;
            }
            default:
                break;
        }
    }
}

int main(int argc, char *argv[]) {
    std::vector<co::fbs::TradeKnockT> out;
    string file = argv[1];
    GetData(file);
    string host = "192.168.101.115";
    int port = 8848;
    string userId = "admin";
    string password = "123456";
    string dbPath = "dfs://SAMPLE_TRDDB";
    string tableName = "QKnockTable";
    DBConnection conn;
    try {
        bool ret = conn.connect(host, port, userId, password);
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
            return 0;
        }
    } catch (exception &ex) {
        cout << "Failed to  connect  with error: " << ex.what();
        return -1;
    }
    cout << "Please waiting..." << endl;
    {
        string script;
        string raw = all_out.front();
        TableSP table = createQKnockTable(raw);
        conn.upload("mt", table);
        script += "login(`admin,`123456);";
        script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
        script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
        script += "db1 = database("", VALUE, 2023.01.01..2023.12.31);";
        script += "db2 = database(\"\", HASH,[STRING,10]);";
        script += "tableName = `QKnockTable;";
        script += "db = database(dbPath,COMPO,[db1,db2],engine=\"TSDB\");";
        script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`date`code,sortColumns=`code`bid_order_no`ask_order_no`date,keepDuplicates=FIRST);";
        script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
        TableSP result = conn.run(script);
        all_out.erase(all_out.begin());
    }

    {
        shared_ptr<BatchTableWriter> btw = make_shared<BatchTableWriter>(host, port, userId, password, true);
        btw->addTable("dfs://SAMPLE_TRDDB", "QKnockTable");
        LOG_INFO << "start insert";
        int64_t start_time = x::NSTimestamp();
        for(auto& raw : all_out) {
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

            btw->insert("dfs://SAMPLE_TRDDB", "QKnockTable"
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
            // LOG_INFO << "insert knock data";
        }
        // btw->removeTable("dfs://SAMPLE_TRDDB", "QKnockTable");
        int64_t end_time = x::NSTimestamp();
        LOG_INFO << "insert num: " << all_out.size() << ", time spread: " << end_time - start_time;
    }

    sleep(3);
    return 0;
    /////////////////////////////
    {
        string script;
        // script += "login(`admin,`123456);";
        script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
        script += "tableName = `QKnockTable;";
        script += "select * from loadTable(dbPath, tableName)";
        TableSP result = conn.run(script);
        // cout << result->getString() << endl;
        cout << "columns: " << result->columns() << endl;
        cout << "size: " << result->size() << endl;
        {
            vector<VectorSP> columnVecs;
            for (int i = 0; i < result->columns(); ++i) {
                columnVecs.push_back(result->getColumn(i));
                VectorSP sp = result->getColumn(i);
                cout << "-------line---------------------------" << endl;
                for(int j = 0; j < result->size(); ++j){
                    cout << "column: " << i << ", " << sp->getString(j) << endl;
                }
            }
        }
    }
    return 0;
}
