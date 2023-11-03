#include <iostream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <boost/program_options.hpp>
#include "write_dolphindb.h"


using namespace std;
using namespace co;
namespace po = boost::program_options;

const string kVersion = "v1.0.10";

int main(int argc, char* argv[]) {
    po::options_description desc("[Broker Server] Usage");
    try {
        desc.add_options()
            ("passwd", po::value<std::string>(), "encode plain password")
            ("help,h", "show help message")
            ("version,v", "show version information");
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
        if (vm.count("passwd")) {
            cout << co::EncodePassword(vm["passwd"].as<std::string>()) << endl;
            return 0;
        } else if (vm.count("help")) {
            cout << desc << endl;
            return 0;
        } else if (vm.count("version")) {
            cout << kVersion << endl;
            return 0;
        }
    } catch (...) {
        cout << desc << endl;
        return 0;
    }
    try {
        LOG_INFO << "kVersion: " <<kVersion;
        Config::Instance();
        shared_ptr<DolphindbWriter> db_writer = make_shared<DolphindbWriter>();
        db_writer->Init();
        while(true) {
            x::Sleep(1000);
        }
        LOG_INFO << "server is stopped.";
    } catch (std::exception& e) {
        LOG_INFO << "server is crashed, " << e.what();
        throw e;
    }
    return 0;
}
