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

TableSP createDemoTable(co::fbs::TradeKnockT knock) {
    vector<string> colNames = { "code","trade_date","trade_time","id","trade_type","fund_id","username","inner_match_no","match_no",
                                "market","name","order_no","batch_no","bs_flag","oc_flag","match_type",
                                "match_volume","match_price","match_amount","error","recv_time"};
    vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_TIME,DT_STRING,DT_LONG,DT_STRING,DT_STRING,DT_STRING,DT_STRING,
                                  DT_LONG,DT_STRING,DT_STRING,DT_STRING,DT_LONG,DT_LONG,DT_LONG,
                                  DT_LONG,DT_DOUBLE,DT_DOUBLE,DT_STRING,DT_LONG};
    int colNum = 21, rowNum = 1;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
    vector<VectorSP> columnVecs;
    columnVecs.reserve(colNum);
    for (int i = 0;i < colNum;i++)
        columnVecs.emplace_back(table->getColumn(i));

    int64_t timestamp = knock.timestamp;
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
        columnVecs[index++]->set(i, Util::createString(knock.code));
        columnVecs[index++]->set(i, Util::createDate(year, month, day));
        columnVecs[index++]->set(i, Util::createTime(hour, min, second, micro_second));
        columnVecs[index++]->set(i, Util::createString(knock.id));
        columnVecs[index++]->set(i, Util::createLong(knock.trade_type));
        columnVecs[index++]->set(i, Util::createString(knock.fund_id));
        columnVecs[index++]->set(i, Util::createString(knock.username));
        columnVecs[index++]->set(i, Util::createString(knock.inner_match_no));
        columnVecs[index++]->set(i, Util::createString(knock.match_no));

        columnVecs[index++]->set(i, Util::createLong(knock.market));
        columnVecs[index++]->set(i, Util::createString(knock.name));
        columnVecs[index++]->set(i, Util::createString(knock.order_no));
        columnVecs[index++]->set(i, Util::createString(knock.batch_no));
        columnVecs[index++]->set(i, Util::createLong(knock.bs_flag));
        columnVecs[index++]->set(i, Util::createLong(knock.oc_flag));
        columnVecs[index++]->set(i, Util::createLong(knock.match_type));

        columnVecs[index++]->set(i, Util::createLong(knock.match_volume));
        columnVecs[index++]->set(i, Util::createDouble(knock.match_price));
        columnVecs[index++]->set(i, Util::createDouble(knock.match_amount));
        columnVecs[index++]->set(i, Util::createString(knock.error));
        columnVecs[index++]->set(i, Util::createLong(knock.recv_time));
    }
    return table;
}

void GetKnock(const string& file, std::vector<co::fbs::TradeKnockT>& out) {
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
            case kFuncFBTradeKnock: {
                auto knock = flatbuffers::GetRoot<co::fbs::TradeKnock>(raw.data());
                co::fbs::TradeKnockT item;
                item.id = knock->id() ? knock->id()->str() : "";
                item.timestamp = knock->timestamp();
                item.trade_type = knock->trade_type();
                item.fund_id = knock->fund_id() ? knock->fund_id()->str() : "";
                item.username = knock->username() ? knock->username()->str() : "";
                item.inner_match_no = knock->inner_match_no() ? knock->inner_match_no()->str() : "";
                item.match_no = knock->match_no() ? knock->match_no()->str() : "";
                item.market = knock->market();
                item.code = knock->code() ? knock->code()->str() : "";
                item.name = knock->name() ? knock->name()->str() : "";
                item.order_no = knock->order_no() ? knock->order_no()->str() : "";
                item.batch_no = knock->batch_no() ? knock->batch_no()->str() : "";
                item.bs_flag = knock->bs_flag();
                item.oc_flag = knock->oc_flag();
                item.match_type = knock->match_type();
                item.match_volume = knock->match_volume();
                item.match_price = knock->match_price();
                item.match_amount = knock->match_amount();
                item.error = knock->error() ? knock->error()->str() : "";
                item.recv_time = knock->recv_time();
//                __info << "code: " << item.code
//                       << ", timestamp: " << item.timestamp
//                       << ", order_no: " << item.order_no
//                       << ", match_no: " << item.match_no
//                       << ", bs_flag: " << item.bs_flag
//                       << ", match_volume: " << item.match_volume
//                       << ", match_amount: " << item.match_amount;
                out.push_back(item);
            }
        }
    }
    LOG_INFO << "knock size: " << out.size();
}

int main(int argc, char *argv[]) {
    std::vector<co::fbs::TradeKnockT> out;
    string file = argv[1];
    GetKnock(file, out);
    string host = "192.168.101.115";
    int port = 8848;
    string userId = "admin";
    string password = "123456";
    string dbPath = "dfs://SAMPLE_TRDDB";
    string tableName = "demoTable";

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
    string script;
    script += "existsTable(\"" + dbPath + "\", `" + tableName + ");";
    TableSP result = conn.run(script);
    cout << "existsTable: " << result->getString() << endl;
    if (result->getString().compare("0") == 0) {
        string script;
        co::fbs::TradeKnockT& knock = out.front();
        TableSP table = createDemoTable(knock);
        conn.upload("mt", table);
        script += "login(`admin,`123456);";
        script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
        script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
        script += "db1 = database("", VALUE, 2023.01.01..2023.12.31);";
        script += "db2 = database(\"\", HASH,[STRING,10]);";
        script += "tableName = `demoTable;";
        script += "db = database(dbPath,COMPO,[db1,db2],engine=\"TSDB\");";
        script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`trade_date`code,sortColumns=`code`fund_id`match_no`trade_date,keepDuplicates=FIRST);";
        script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
        TableSP result = conn.run(script);
        out.erase(out.begin());
    }

    {
        shared_ptr<BatchTableWriter> btw = make_shared<BatchTableWriter>(host, port, userId, password, true);
        btw->addTable("dfs://SAMPLE_TRDDB", "demoTable");
        LOG_INFO << "start insert";
        int64_t start_time = x::NSTimestamp();
        for(auto& it : out) {
            int64_t timestamp = it.timestamp;
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
            btw->insert("dfs://SAMPLE_TRDDB", "demoTable"
                    , Util::createString(it.code)
                    , Util::createDate(year, month, day)
                    , Util::createTime(hour, min, second, micro_second)
                    , Util::createString(it.id)
                    , Util::createLong(it.trade_type)
                    , Util::createString(it.fund_id)
                    , Util::createString(it.username)
                    , Util::createString(it.inner_match_no)
                    , Util::createString(it.match_no)
                    , Util::createLong(it.market)
                    , Util::createString(it.name)
                    , Util::createString(it.order_no)
                    , Util::createString(it.batch_no)
                    , Util::createLong(it.bs_flag)
                    , Util::createLong(it.oc_flag)
                    , Util::createLong(it.match_type)
                    , Util::createLong(it.match_volume)
                    , Util::createDouble(it.match_price)
                    , Util::createDouble(it.match_amount)
                    , Util::createString(it.error)
                    , Util::createLong(it.recv_time)
            );
            // LOG_INFO << "insert knock data";
        }
        // btw->removeTable("dfs://SAMPLE_TRDDB", "demoTable");
        int64_t end_time = x::NSTimestamp();
        LOG_INFO << "insert num: " << out.size() << ", time spread: " << end_time - start_time;
    }

    sleep(3);
    return 0;
    /////////////////////////////
    {
        string script;
        // script += "login(`admin,`123456);";
        script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
        script += "tableName = `demoTable;";
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
