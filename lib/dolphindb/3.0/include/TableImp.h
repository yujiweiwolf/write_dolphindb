// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <atomic>

#include "Table.h"
namespace dolphindb {

class AbstractTable : public Table {
public:
	explicit AbstractTable(const std::vector<std::string> &colNames);
	AbstractTable(const std::vector<std::string> &colNames, const std::shared_ptr<std::unordered_map<std::string,int>>& colMap);
	~AbstractTable() override = default;
	ConstantSP getColumn(const std::string& name) const override;
	ConstantSP getColumn(const std::string& qualifier, const std::string& name) const override;
	ConstantSP getColumn(const std::string& name, const ConstantSP& rowFilter) const override;
	ConstantSP getColumn(const std::string& qualifier, const std::string& name, const ConstantSP& rowFilter) const override;
	ConstantSP getColumn(INDEX index, const ConstantSP& rowFilter) const override;
	ConstantSP getColumn(INDEX index) const override = 0;
	ConstantSP get(INDEX col, INDEX row) const override = 0;
	INDEX columns() const override {return static_cast<INDEX>(colNames_.size());}
	const std::string& getColumnName(int index) const override {return colNames_.at(index);}
	const std::string& getColumnQualifier(int index) const override
	{
		std::ignore = index;
		return name_;
	}
	void setColumnName(int index, const std::string& name) override;
	int getColumnIndex(const std::string& name) const override;
	bool contain(const std::string& name) const override;
	bool contain(const std::string& qualifier, const std::string& name) const override;
	ConstantSP getColumnLabel() const override;
	ConstantSP values() const override;
	ConstantSP keys() const override { return getColumnLabel();}
	void setName(const std::string& name) override{name_=name;}
	const std::string& getName() const override { return name_;}
	virtual bool isTemporary() const {return false;}
	virtual void setTemporary(bool temp){std::ignore = temp;}
	bool sizeable() const override {return false;}
	std::string getString(INDEX index) const override;
	std::string getString() const override;
	ConstantSP get(INDEX index) const override { return getInternal(index);}
	bool set(INDEX index, const ConstantSP& value) override;
	ConstantSP get(const ConstantSP& index) const override { return getInternal(index);}
	ConstantSP getWindow(int colStart, int colLength, int rowStart, int rowLength) const override {return getWindowInternal(colStart, colLength, rowStart, rowLength);}
	ConstantSP getMember(const ConstantSP& key) const override { return getMemberInternal(key);}
	ConstantSP getInstance() const override {return getInstance(0);}
	ConstantSP getInstance(int size) const override;
	ConstantSP getValue() const override;
	ConstantSP getValue(INDEX capacity) const override;
	bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, std::string& errMsg) override;
	bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<std::string>& colNames, std::string& errMsg) override;
	bool remove(const ConstantSP& indexSP, std::string& errMsg) override;
	ConstantSP getSubTable(std::vector<int> indices) const override = 0;
	COMPRESS_METHOD getColumnCompressMethod(INDEX index) override;
	void setColumnCompressMethods(const std::vector<COMPRESS_METHOD> &methods) override;
	bool clear() override =0;
	void updateSize() override = 0;
protected:
	ConstantSP getInternal(INDEX index) const;
	ConstantSP getInternal(const ConstantSP& index) const;
	ConstantSP getWindowInternal(int colStart, int colLength, int rowStart, int rowLength) const;
	ConstantSP getMemberInternal(const ConstantSP& key) const;

private:
	std::string getTableClassName() const;
	std::string getTableTypeName() const;

protected:
	std::shared_ptr<std::unordered_map<std::string,int>> colMap_;
	std::string name_;
	std::vector<COMPRESS_METHOD> colCompresses_;
};

class BasicTable: public AbstractTable{
public:
	BasicTable(const std::vector<ConstantSP>& cols, const std::vector<std::string>& colNames, const std::vector<int>& keys);
	BasicTable(const std::vector<ConstantSP>& cols, const std::vector<std::string>& colNames);
	~BasicTable() override = default;
	virtual bool isBasicTable() const {return true;}
	ConstantSP getColumn(INDEX index) const override;
	ConstantSP get(INDEX col, INDEX row) const override {return cols_[col]->get(row);}
	DATA_TYPE getColumnType(int index) const override { return cols_[index]->getType();}
	void setColumnName(int index, const std::string& name) override;
	INDEX size() const override {return size_;}
	bool sizeable() const override {return isReadOnly()==false;}
	bool set(INDEX index, const ConstantSP& value) override;
	ConstantSP get(INDEX index) const override;
	ConstantSP get(const ConstantSP& index) const override;
	ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP getInstance(int size) const override;
	ConstantSP getValue() const override;
	ConstantSP getValue(INDEX capacity) const override;
	bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, std::string& errMsg) override;
	bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<std::string>& colNames, std::string& errMsg) override;
	bool remove(const ConstantSP& indexSP, std::string& errMsg) override;
	long long getAllocatedMemory() const override;
	TABLE_TYPE getTableType() const override {return BASICTBL;}
	void drop(std::vector<int>& columns) override;
	bool join(std::vector<ConstantSP>& columns);
	bool clear() override;
	void updateSize() override;
	ConstantSP getSubTable(std::vector<int> indices) const override;

private:
	bool increaseCapacity(long long newCapacity, std::string& errMsg);
	void initData(const std::vector<ConstantSP>& cols, const std::vector<std::string>& colNames);
	//bool internalAppend(std::vector<ConstantSP>& values, string& errMsg);
	bool internalRemove(const ConstantSP& indexSP, std::string& errMsg);
	void internalDrop(std::vector<int>& columns);
	bool internalUpdate(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<std::string>& colNames, std::string& errMsg);


	std::vector<ConstantSP> cols_;
	//bool readOnly_;
	INDEX size_;
	INDEX capacity_;
};

using BasicTableSP = SmartPointer<BasicTable>;

} // namespace dolphindb
