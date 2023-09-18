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

TableSP createQOrderTable(std::string& raw) {
    vector<string> colNames = { "code","date","time","order_no","bs_flag","order_type","order_price","order_volume","recv_time"};
    vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_TIME,DT_LONG,DT_CHAR,DT_CHAR,DT_DOUBLE,DT_LONG,DT_LONG};
    int colNum = 9, rowNum = 1;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
    vector<VectorSP> columnVecs;
    columnVecs.reserve(colNum);
    for (int i = 0;i < colNum;i++)
        columnVecs.emplace_back(table->getColumn(i));

    auto q = flatbuffers::GetRoot<co::fbs::QOrder>(raw.data());
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
        columnVecs[index++]->set(i, Util::createLong(q->order_no()));
        columnVecs[index++]->set(i, Util::createChar(q->bs_flag()));
        columnVecs[index++]->set(i, Util::createChar(q->order_type()));
        columnVecs[index++]->set(i, Util::createDouble(q->order_price()));
        columnVecs[index++]->set(i, Util::createLong(q->order_volume()));
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
//                auto q = flatbuffers::GetRoot<co::fbs::QTick>(raw.data());
//                co::fbs::QTickT m;
//                m.code = q->code() ? q->code()->str() : "";
//                m.name = q->name() ? q->name()->str() : "";
//                m.src = q->src();
//                m.dtype = q->dtype();
//                m.timestamp = q->timestamp();
//                m.market = q->market();
//                m.pre_close = q->pre_close();
//                m.upper_limit = q->upper_limit();
//                m.lower_limit = q->lower_limit();
//                auto bps = q->bp();
//                auto bvs = q->bv();
//                auto aps = q->ap();
//                auto avs = q->av();
//                auto ap_size = q->ap()->size();
//                for (size_t i = 0; i < 10 && bps && bvs && i < bps->size() && i < bvs->size(); ++i) {
//                    double bp = bps->Get(i);
//                    int64_t bv = bvs->Get(i);
//                    LOG_INFO << "bp: " << bp << ", bv: " << bv;
//                }
//                for (size_t i = 0; i < 10 && aps && avs && i < aps->size() && i < avs->size(); ++i) {
//                    double ap = aps->Get(i);
//                    int64_t av = avs->Get(i);
//                    m.ap.push_back(ap);
//                    m.av.push_back(av);
//                    LOG_INFO << "ap: " << ap << ", av: " << av;
//                }
//                m.status = q->lower_limit();
//                m.new_price = q->lower_limit();
//                m.new_volume = q->lower_limit();
//                m.new_amount = q->lower_limit();
//                m.sum_volume = q->lower_limit();
//                m.sum_amount = q->lower_limit();
//                m.open = q->lower_limit();
//                m.high = q->lower_limit();
//                m.low = q->lower_limit();
//                m.avg_bid_price = q->lower_limit();
//                m.avg_ask_price = q->lower_limit();
                break;
            }
            case kFBPrefixQOrder: {
                all_out.push_back(raw);
//                auto q = flatbuffers::GetRoot<co::fbs::QOrder>(raw.data());
//                std::string code = q->code() ? q->code()->str() : "";
//                int64_t date = q->date() > 0 ? q->date() : q->timestamp() / 1000000000LL;
//                int64_t timestamp = q->timestamp();
//                int64_t sum_volume = q->sum_volume();
//                int64_t sum_amount = q->sum_amount();
//                int64_t new_volume = q->new_volume();
//                int64_t new_amount = q->new_amount();
//                LOG_INFO << "order code: " << code;
                break;
            }
            case kFBPrefixQKnock: {
                auto q = flatbuffers::GetRoot<co::fbs::QKnock>(raw.data());
                std::string code = q->code() ? q->code()->str() : "";
//                int64_t date = q->date() > 0 ? q->date() : q->timestamp() / 1000000000LL;
//                int64_t timestamp = q->timestamp();
//                int64_t sum_volume = q->sum_volume();
//                int64_t sum_amount = q->sum_amount();
//                int64_t new_volume = q->new_volume();
//                int64_t new_amount = q->new_amount();
//                LOG_INFO << "konck code: " << code;
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
    string tableName = "QOrderTable";
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
        TableSP table = createQOrderTable(raw);
        conn.upload("mt", table);
        script += "login(`admin,`123456);";
        script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
        script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
        script += "db1 = database("", VALUE, 2023.01.01..2023.12.31);";
        script += "db2 = database(\"\", HASH,[STRING,10]);";
        script += "tableName = `QOrderTable;";
        script += "db = database(dbPath,COMPO,[db1,db2],engine=\"TSDB\");";
        script += "date = db.createPartitionedTable(mt,tableName, partitionColumns=`date`code,sortColumns=`code`order_no`date,keepDuplicates=FIRST);";
        script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
        TableSP result = conn.run(script);
        all_out.erase(all_out.begin());
    }

    {
        shared_ptr<BatchTableWriter> btw = make_shared<BatchTableWriter>(host, port, userId, password, true);
        btw->addTable("dfs://SAMPLE_TRDDB", "QOrderTable");
        LOG_INFO << "start insert";
        int64_t start_time = x::NSTimestamp();
        for(auto& raw : all_out) {
            auto q = flatbuffers::GetRoot<co::fbs::QOrder>(raw.data());
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

            btw->insert("dfs://SAMPLE_TRDDB", "QOrderTable"
                    , Util::createString(code)
                    , Util::createDate(year, month, day)
                    , Util::createTime(hour, min, second, micro_second)
                    , Util::createLong(q->order_no())
                    , Util::createChar(q->bs_flag())
                    , Util::createChar(q->order_type())
                    , Util::createDouble(q->order_price())
                    , Util::createLong(q->order_volume())
                    , Util::createLong(q->recv_time())
            );
            // LOG_INFO << "insert knock data";
        }
        // btw->removeTable("dfs://SAMPLE_TRDDB", "QOrderTable");
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
        script += "tableName = `QOrderTable;";
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
