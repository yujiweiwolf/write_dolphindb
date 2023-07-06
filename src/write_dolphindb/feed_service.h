#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <memory>
#include <vector>
#include <map>

#include "x/x.h"
#include "coral/define.h"
#include "coral/queues.h"

namespace co {

    /**
     * 行情服务
     * @author Guangxu Pan, bajizhh@gmail.com
     * @since 2014-05-21 14:42:13
     * @version 2021-10-26 14:23:18
     */
    class MyFeedService {
    public:
        MyFeedService();
        ~MyFeedService();

        void Init(const std::string& gateway, const std::string& service = "");
        void Start();
        void Join();
        void Stop();

        std::string GetQTick(const std::string& code);
        void GetQTick(std::vector<std::string>* raws, const std::vector<std::string>& codes = {});

        void SubQTick(const std::string& code);
        void SubQEtfIopv(const std::string& code);

        bool disable_spot() const;
        void set_disable_spot(bool value);
        bool disable_future() const;
        void set_disable_future(bool value);
        bool disable_option() const;
        void set_disable_option(bool value);
        bool disable_index() const;
        void set_disable_index(bool value);

        const std::map<int8_t, bool>& include_markets() const;
        std::map<int8_t, bool>* mutable_include_markets();
        const std::map<int8_t, bool>& exclude_markets() const;
        std::map<int8_t, bool>* mutable_exclude_markets();

        int64_t Pop(std::string* data, int64_t timeout_ns = 0, int64_t sleep_ns = 0);
        std::shared_ptr<StringQueue> queue();
        void set_queue(std::shared_ptr<StringQueue> queue);

    private:
        class MyFeedServiceImpl;
        MyFeedServiceImpl* m_ = nullptr;
    };

    typedef std::shared_ptr<MyFeedService> MyFeedServicePtr;

}
