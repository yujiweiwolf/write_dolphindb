// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

#include "Constant.h"
#include "SmartPointer.h"
namespace dolphindb {

class EXPORT_DECL DFSChunkMeta : public Constant{
public:
    DFSChunkMeta(std::string path, const Guid& id, int version, int size, CHUNK_TYPE chunkType, const std::vector<std::string>& sites, long long cid);
    DFSChunkMeta(std::string path, const Guid& id, int version, int size, CHUNK_TYPE chunkType, const std::string* sites, int siteCount, long long cid);
    explicit DFSChunkMeta(const DataInputStreamSP& in);
    ~DFSChunkMeta() override;
    int size() const override {return size_;}
    std::string getString() const override;
    long long getAllocatedMemory() const override;
    ConstantSP getMember(const ConstantSP& key) const override;
    ConstantSP get(const ConstantSP& index) const override {return getMember(index);}
    ConstantSP keys() const override;
    ConstantSP values() const override;
    DATA_TYPE getType() const override {return DT_DICTIONARY;}
    DATA_TYPE getRawType() const override {return DT_DICTIONARY;}
    DATA_CATEGORY getCategory() const override {return MIXED;}
    ConstantSP getInstance() const override {return getValue();}
    ConstantSP getValue() const override {return new DFSChunkMeta(path_, id_, version_, size_, (CHUNK_TYPE)type_, sites_, replicaCount_, cid_);}
    const std::string& getPath() const {return path_;}
    const Guid& getId() const {return id_;}
    long long getCommitId() const {return cid_;}
    void setCommitId(long long cid) { cid_ = cid;}
    int getVersion() const {return version_;}
    void setVersion(int version){version_ = version;}
    void setSize(int sz){size_ = sz;}
    int getCopyCount() const {return replicaCount_;}
    const std::string& getCopySite(int index) const {return sites_[index];}
    bool isTablet() const { return type_ == TABLET_CHUNK;}
    bool isFileBlock() const { return type_ == FILE_CHUNK;}
    bool isSplittable() const { return type_ == SPLIT_TABLET_CHUNK;}
    bool isSmallFileBlock() const {return type_ == SMALLFILE_CHUNK;}
    CHUNK_TYPE getChunkType() const {return (CHUNK_TYPE)type_;}

protected:
    ConstantSP getAttribute(const std::string& attr) const;
    ConstantSP getSiteVector() const;

private:
    char type_;
    char replicaCount_;
    int version_;
    int size_;
    std::string* sites_;
    std::string path_;
    long long cid_;
    Guid id_;
};
using DFSChunkMetaSP = SmartPointer<DFSChunkMeta>;
} // namespace dolphindb

#ifdef _MSC_VER
#pragma warning( pop )
#endif
