// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#if defined(_MSC_VER)
#pragma warning( push )
#pragma warning( disable : 4100 )
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#else // gcc
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#include "Constant.h"
#include "SmartPointer.h"
namespace dolphindb {

class EXPORT_DECL Dictionary:public Constant{
public:
    Dictionary() : Constant(1283){}
    ~Dictionary() override = default;
    INDEX size() const override = 0;
    virtual INDEX count() const = 0;
    virtual void clear()=0;
    ConstantSP getMember(const ConstantSP& key) const override =0;
    virtual ConstantSP getMember(const std::string& key) const {throw RuntimeException("String key not supported");}
    virtual ConstantSP get(INDEX column, INDEX row){throw RuntimeException("Dictionary does not support cell function");}
    virtual DATA_TYPE getKeyType() const = 0;
    virtual DATA_CATEGORY getKeyCategory() const = 0;
    DATA_TYPE getType() const override = 0;
    DATA_CATEGORY getCategory() const override = 0;
    ConstantSP keys() const override = 0;
    ConstantSP values() const override = 0;
    std::string getString() const override = 0;
    std::string getScript() const override {return "dict()";}
    std::string getString(int index) const override {throw RuntimeException("Dictionary::getString(int index) not supported");}
    virtual bool remove(const ConstantSP& key) = 0;
    using Constant::set;
    bool set(const ConstantSP& key, const ConstantSP& value) override =0;
    virtual bool set(const std::string& key, const ConstantSP& value){throw RuntimeException("String key not supported");}
    using Constant::get;
    ConstantSP get(const ConstantSP& key) const override {return getMember(key);}
    virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const = 0;
    bool isLargeConstant() const override {return true;}
};

using DictionarySP = SmartPointer<Dictionary>;
} // namespace dolphindb

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
