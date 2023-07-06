#include <iostream>
#include <string>
#include <thread>

#include "feed_service.h"
#include "flatbuffers/flatbuffers.h"
#include "coral/feeder_generated.h"
#include "coral/sys_generated.h"
// #include "http.h"

namespace co {

    class MyFeedService::MyFeedServiceImpl {
    public:
        MyFeedServiceImpl();
        ~MyFeedServiceImpl();

        void Init(const std::string& gateway, const std::string& service);
        void Start();
        void Stop();
        void Join();

        void Run();
        void Sub(const std::string& prefix);

        std::string service_;  // API地址

        bool quit_ = false;
        int64_t node_type_ = 0; // 行情节点类型

        bool disable_spot_ = false; // 是否启用现货行情快照
        bool disable_future_ = false; // 是否启用期货行情快照
        bool disable_option_ = false; // 是否启用期权行情快照
        bool disable_index_ = false; // 是否启用指数行情快照

        std::map<int8_t, bool> include_markets_; // 包含的市场代码
        std::map<int8_t, bool> exclude_markets_; // 排除的市场代码

        x::ZMQ sock_;
        std::shared_ptr<StringQueue> queue_ = nullptr;

        std::shared_ptr<std::thread> thread_ = nullptr;
    };

    MyFeedService::MyFeedServiceImpl::MyFeedServiceImpl():
        sock_(ZMQ_SUB),
        queue_(std::make_shared<StringQueue>()) {

    }

    MyFeedService::MyFeedServiceImpl::~MyFeedServiceImpl() {
        Stop();
    }

    MyFeedService::MyFeedService() : m_(new MyFeedServiceImpl()) {

    }

    MyFeedService::~MyFeedService() {
        delete m_;
    }

    void MyFeedService::MyFeedServiceImpl::Init(const std::string& gateway, const std::string& service) {
        if (!gateway.empty()) {
            sock_.SetSockOpt(ZMQ_LINGER, 0);
            sock_.SetSockOpt(ZMQ_RCVHWM, 0);
            std::vector<std::string> urls;
            x::Split(&urls, gateway, ",");
            for (auto url: urls) {
                if (!url.empty()) {
                    sock_.Connect(url);
                }
            }
        }
        service_ = service;
    }

    void MyFeedService::Init(const std::string& gateway, const std::string& service) {
        m_->Init(gateway, service);
    }

    void MyFeedService::MyFeedServiceImpl::Start() {
        thread_ = std::make_shared<std::thread>(std::bind(&MyFeedServiceImpl::Run, this));
    }

    void MyFeedService::Start() {
        m_->Start();
    }

    void MyFeedService::MyFeedServiceImpl::Stop() {
        quit_ = true;
        sock_.Close();
        if (thread_) {
            thread_->join();
            thread_ = nullptr;
        }
    }

    void MyFeedService::Stop() {
        m_->Stop();
    }

    void MyFeedService::MyFeedServiceImpl::Join() {
        if (thread_) {
            thread_->join();
        }
    }

    void MyFeedService::Join() {
        m_->Join();
    }

    void MyFeedService::MyFeedServiceImpl::Run() {
        bool filter_dtype = disable_spot_ || disable_future_ || disable_option_ || disable_index_;
        bool filter_market = !include_markets_.empty() || !exclude_markets_.empty();
        while (!quit_) {
            std::string line = sock_.Recv();
            if (line.empty()) { // timeout
                continue;
            }
            int len = (int)line.size();
            if (len < 5) {
                continue;
            }
            const char* p = line.data();
            // a\t510050.SH\t<fb>
            int pos1 = 1;
            if (p[pos1] != kSep) {
                continue;
            }
            int pos2 = (int)line.find(kSep, pos1 + 1);
            if (pos2 < 0) {
                continue;
            }
            char prefix = p[0];
            // string code = line.substr(pos1 + 1, pos2 - pos1 - 1);
            std::string raw = line.substr(pos2 + 1);
//            if (node_type_ != 0) { // 填写当前行情节点的接收时间，(node_type, begin_time, end_time)
//                switch (prefix) {
//                    case kFBPrefixQTick: {
//                        auto q = flatbuffers::GetMutableRoot<co::fbs::QTick>((void*)raw.data());
//                        auto traces = q->mutable_traces();
//                        if (traces) {
//                            for (int i = 0; i + 3 < (int)traces->size(); i += 3) {
//                                if (traces->Get(i) == 0) {
//                                    traces->Mutate(i + 1, x::NSTimestamp());
//                                    break;
//                                }
//                            }
//                        }
//                        break;
//                    }
//                    case kFBPrefixQEtfIopv:
//                    {
//                        auto q = flatbuffers::GetMutableRoot<co::fbs::QEtfIopv>((void*)raw.data());
//                        auto traces = q->mutable_traces();
//                        if (traces) {
//                            for (int i = 0; i + 3 < (int)traces->size(); i += 3) {
//                                if (traces->Get(i) == 0) {
//                                    traces->Mutate(i + 1, x::NSTimestamp());
//                                    break;
//                                }
//                            }
//                        }
//                        break;
//                    }
//                    default:
//                        break;
//                }
//            }
            if (prefix == kFBPrefixQTick && (filter_dtype || filter_market)) {
                bool pass = true;
                auto q = flatbuffers::GetRoot<co::fbs::QTick>(raw.data());
                if (filter_dtype) {
                    switch (q->dtype()) {
                        case kDTypeStock: {
                            if (disable_spot_) {
                                pass = false;
                            }
                            break;
                        }
                        case kDTypeFuture: {
                            if (disable_future_) {
                                pass = false;
                            }
                            break;
                        }
                        case kDTypeOption: {
                            if (disable_option_) {
                                pass = false;
                            }
                            break;
                        }
                        case kDTypeIndex: {
                            if (disable_index_) {
                                pass = false;
                            }
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                }
                if (filter_market && pass) {
                    int8_t market = q->market();
                    if (!include_markets_.empty() && include_markets_.find(market) == include_markets_.end()) {
                        pass = false;
                    }
                    if (pass && !exclude_markets_.empty() && exclude_markets_.find(market) != exclude_markets_.end()) {
                        pass = false;
                    }
                }
                if (!pass) {
                    continue;
                }
            }
            queue_->Push((int64_t)prefix, raw);
        }
    }

    void MyFeedService::MyFeedServiceImpl::Sub(const std::string& prefix) {
        sock_.SetSockOpt(ZMQ_SUBSCRIBE, prefix);
    }

    void MyFeedService::SubQTick(const std::string& code) {
        {
            std::stringstream ss;
            ss << kFBPrefixQTick << kSep << code;
            m_->Sub(ss.str());
            x::Sleep(100);
        }
    }

    void MyFeedService::SubQEtfIopv(const std::string& code) {
        std::stringstream ss;
        ss << kFBPrefixQEtfIopv << kSep << code;
        m_->Sub(ss.str());
    }

    bool MyFeedService::disable_spot() const {
        return m_->disable_spot_;
    }

    void MyFeedService::set_disable_spot(bool value) {
        m_->disable_spot_ = value;
    }

    bool MyFeedService::disable_future() const {
        return m_->disable_future_;
    }

    void MyFeedService::set_disable_future(bool value) {
        m_->disable_future_ = value;
    }

    bool MyFeedService::disable_option() const {
        return m_->disable_option_;
    }

    void MyFeedService::set_disable_option(bool value) {
        m_->disable_option_ = value;
    }

    bool MyFeedService::disable_index() const {
        return m_->disable_index_;
    }

    void MyFeedService::set_disable_index(bool value) {
        m_->disable_index_ = value;
    }

    const map<int8_t, bool>& MyFeedService::include_markets() const {
        return m_->include_markets_;
    }

    map<int8_t, bool>* MyFeedService::mutable_include_markets() {
        return &m_->include_markets_;
    }

    const map<int8_t, bool>& MyFeedService::exclude_markets() const {
        return m_->exclude_markets_;
    }

    map<int8_t, bool>* MyFeedService::mutable_exclude_markets() {
        return &m_->exclude_markets_;
    }

    int64_t MyFeedService::Pop(std::string* data, int64_t timeout_ns, int64_t sleep_ns) {
        return m_->queue_->Pop(data, timeout_ns, sleep_ns);
    }

    std::shared_ptr<StringQueue> MyFeedService::queue() {
        return m_->queue_;
    }

    void MyFeedService::set_queue(std::shared_ptr<StringQueue> queue) {
        m_->queue_ = queue;
    }

    std::string MyFeedService::GetQTick(const std::string& code) {
        std::vector<std::string> raws;
        GetQTick(&raws, {code});
        return !raws.empty() ? raws[0] : "";
    }

    void MyFeedService::GetQTick(std::vector<std::string>* raws, const std::vector<std::string>& codes) {
        // http://pfdb11:8001/feed_gateway/getQTick?cluster_token=f1cc0713892a4b1dac023e4530cdcf1b1&code=510300.SH&format=json
        // http://xmac:8001/feed_gateway/getQTick?cluster_token=f1cc0713892a4b1dac023e4530cdcf1b1&code=510300.SH&format=json
//        std::stringstream ss;
//        for (auto itr = codes.begin(); itr != codes.end(); ++itr) {
//            if (itr != codes.begin()) {
//                ss << ",";
//            }
//            ss << *itr;
//        }
//        std::string codes_str = ss.str();
//        x::URL url;
//        x::URL::Parse(&url, m_->service_);
//        ss.str("");
//        ss << url.scheme() << "://" << url.host() << ":" << url.port() << "/feed_gateway/getQTick";
//        std::string address = ss.str();
//        std::map<std::string, std::string> params = {
//            {"cluster_token", url.GetStr("cluster_token")},
//            {"code", codes_str},
//        };
//        std::string body = HTTPGet(address, params);
//        if (body.empty()) {
//            throw std::runtime_error("empty response");
//        }
//        auto msg = flatbuffers::GetRoot<co::fbs::GetFeedDataMessage>(body.data());
//        if (msg->error() && msg->error()->size() > 0) {
//            throw std::logic_error(msg->error()->str());
//        }
//        auto items = msg->items();
//        if (items) {
//            for (auto arr: *items) {
//                if (arr && arr->data() && arr->data()->size() > 0) {
//                    std::string raw((const char*)arr->data()->data(), arr->data()->size());
//                    raws->emplace_back(raw);
//                }
//            }
//        }
    }

}
