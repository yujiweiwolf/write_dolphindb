// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#if defined(_MSC_VER)
#pragma warning( push )
#pragma warning( disable : 4100 4251 )
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#else // gcc
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#include "Constant.h"
#include "SmartPointer.h"
#include "json/json.h"

namespace dolphindb {

class EXPORT_DECL Table: public Constant{
public:
    Table() : Constant(1539){}
    ~Table() override = default;
    std::string getScript() const override {return getName();}
    virtual ConstantSP getColumn(const std::string& name) const = 0;
    virtual ConstantSP getColumn(const std::string& qualifier, const std::string& name) const = 0;
    ConstantSP getColumn(INDEX index) const override = 0;
    virtual ConstantSP getColumn(const std::string& name, const ConstantSP& rowFilter) const = 0;
    virtual ConstantSP getColumn(const std::string& qualifier, const std::string& name, const ConstantSP& rowFilter) const = 0;
    virtual ConstantSP getColumn(INDEX index, const ConstantSP& rowFilter) const = 0;
    INDEX columns() const override = 0;
    virtual const std::string& getColumnName(int index) const = 0;
    virtual const std::string& getColumnQualifier(int index) const = 0;
    virtual void setColumnName(int index, const std::string& name)=0;
    virtual int getColumnIndex(const std::string& name) const = 0;
    virtual DATA_TYPE getColumnType(int index) const = 0;
    virtual bool contain(const std::string& name) const = 0;
    virtual bool contain(const std::string& qualifier, const std::string& name) const = 0;
    virtual void setName(const std::string& name)=0;
    virtual const std::string& getName() const = 0;
    ConstantSP get(INDEX index) const override {return getColumn(index);}
    ConstantSP get(const ConstantSP& index) const override = 0;
    virtual ConstantSP getValue(INDEX capacity) const = 0;
    ConstantSP getValue() const override = 0;
    using Constant::getInstance;
    virtual ConstantSP getInstance(INDEX size) const = 0;
    INDEX size() const override = 0;
    bool sizeable() const override = 0;
    std::string getString(INDEX index) const override = 0;
    std::string getString() const override = 0;
    nlohmann::json getRowJson(size_t index);
    ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const override = 0;
    ConstantSP getMember(const ConstantSP& key) const override = 0;
    ConstantSP values() const override = 0;
    ConstantSP keys() const override = 0;
    virtual TABLE_TYPE getTableType() const = 0;
    virtual void drop(std::vector<int>& columnVec) {throw RuntimeException("Table::drop() not supported");}
    virtual bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<std::string>& colNames, std::string& errMsg) = 0;
    virtual bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, std::string& errMsg) = 0;
    virtual bool remove(const ConstantSP& indexSP, std::string& errMsg) = 0;
    DATA_TYPE getType() const override {return DT_DICTIONARY;}
    DATA_TYPE getRawType() const override {return DT_DICTIONARY;}
    DATA_CATEGORY getCategory() const override {return MIXED;}
    bool isLargeConstant() const override {return true;}
    virtual void release() const {}
    virtual void checkout() const {}
    long long getAllocatedMemory() const override = 0;
    virtual ConstantSP getSubTable(std::vector<int> indices) const = 0;
    virtual COMPRESS_METHOD getColumnCompressMethod(INDEX index) = 0;
    virtual void setColumnCompressMethods(const std::vector<COMPRESS_METHOD> &methods) = 0;
    virtual bool clear()=0;
    virtual void updateSize() = 0;
protected:
	std::vector<std::string> colNames_;
};
using TableSP = SmartPointer<Table>;
} // namespace dolphindb

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
