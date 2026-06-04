// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Domain.h"
#include "Constant.h"
#include "Dictionary.h"

namespace dolphindb{

class HashDomain : public Domain{
public:
    HashDomain(DATA_TYPE partitionColType, const ConstantSP &partitionSchema) : Domain(HASH, partitionColType){
        buckets_ = partitionSchema->getInt();
    }

	std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;

private:
    int buckets_;
};

class ListDomain : public Domain {
public:
    ListDomain(DATA_TYPE partitionColType, const ConstantSP& partitionSchema);

    std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;

private:
	DictionarySP dict_;
};


class ValueDomain : public Domain{
public:
	ValueDomain(DATA_TYPE partitionColType, const ConstantSP &partitionSchema) : Domain(VALUE, partitionColType){ std::ignore = partitionSchema; }
	
	std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;
};

class RangeDomain : public Domain{
public:
    RangeDomain(DATA_TYPE partitionColType, const ConstantSP &partitionSchema) : Domain(RANGE, partitionColType), range_(partitionSchema){ }
	
	std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;
private:
    VectorSP range_;
};

} // namespace dolphindb
