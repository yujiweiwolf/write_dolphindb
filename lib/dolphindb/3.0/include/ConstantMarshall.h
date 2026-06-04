// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "SysIO.h"
#include "Constant.h"

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

#define MARSHALL_BUFFER_SIZE 4096

namespace dolphindb {

class CodeMarshall;
class CodeUnmarshall;
class ConstantMarshallFactory;
class ConstantUnmarshallFactory;
class SymbolBaseUnmarshall;
class SymbolBaseMarshall;

using CodeMarshallSP = SmartPointer<CodeMarshall>;
using CodeUnmarshallSP = SmartPointer<CodeUnmarshall>;
using ConstantMarshallFactorySP = SmartPointer<ConstantMarshallFactory>;
using ConstantUnmarshallFactorySP = SmartPointer<ConstantUnmarshallFactory>;
using SymbolBaseUnmarshallSP = SmartPointer<SymbolBaseUnmarshall>;
using SymbolBaseMarshallSP = SmartPointer<SymbolBaseMarshall>;

class EXPORT_DECL ConstantMarshall {
public:
	virtual ~ConstantMarshall()= default;
	virtual bool start(const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret)=0;
	virtual bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret)=0;
	virtual void reset() = 0;
	virtual IO_ERR flush() = 0;
};

class EXPORT_DECL ConstantUnmarshall{
public:
	virtual ~ConstantUnmarshall()= default;
	virtual bool start(uint16_t flag, bool blocking, IO_ERR& ret)=0;
	virtual void reset() = 0;
	ConstantSP getConstant(){return obj_;}
	
protected:
	ConstantSP obj_;
};

using ConstantMarshallSP = SmartPointer<ConstantMarshall>;
using ConstantUnmarshallSP = SmartPointer<ConstantUnmarshall>;

class EXPORT_DECL ConstantMarshallImp : public ConstantMarshall {
public:
	explicit ConstantMarshallImp(const DataOutputStreamSP& out):out_(out) {}
	~ConstantMarshallImp() override = default;
	bool start(const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override{
		return start(nullptr, 0, target, blocking, compress, ret);
	}
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override =0;
	IO_ERR flush() override;

protected:
	short encodeFlag(const ConstantSP& target, bool compress = false);

	BufferWriter<DataOutputStreamSP> out_;
	ConstantSP target_;
	bool complete_{false};
	char buf_[MARSHALL_BUFFER_SIZE];
};

class EXPORT_DECL ConstantUnmarshallImp : public ConstantUnmarshall{
public:
	explicit ConstantUnmarshallImp(const DataInputStreamSP& in):in_(in){}
	~ConstantUnmarshallImp() override = default;

protected:
	void decodeFlag(uint16_t flag, DATA_FORM& form, DATA_TYPE& type);

	DataInputStreamSP in_;
};

class EXPORT_DECL SymbolBaseMarshall {
public:
	explicit SymbolBaseMarshall(const DataOutputStreamSP& out): out_(out) {}
	~SymbolBaseMarshall()= default;
	bool start(const SymbolBaseSP& target, bool blocking, IO_ERR& ret);
	void reset();

private:
	BufferWriter<DataOutputStreamSP> out_;
	SymbolBaseSP target_;
	bool complete_{false};
	int nextStart_{0};
	int partial_{0};
	char buf_[MARSHALL_BUFFER_SIZE];
	int dict_{0};
};

class EXPORT_DECL ScalarMarshall: public ConstantMarshallImp{
public:
	explicit ScalarMarshall(const DataOutputStreamSP& out):ConstantMarshallImp(out) {}
	~ScalarMarshall() override = default;
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	void reset() override;
private:
	int partial_{0};
};

class EXPORT_DECL VectorMarshall: public ConstantMarshallImp{
public:
	explicit VectorMarshall(const DataOutputStreamSP& out):ConstantMarshallImp(out){}
	~VectorMarshall() override = default;
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	bool start(const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	void reset() override;
	void resetSymbolBaseMarshall(bool createIfNotExist);
	COMPRESS_METHOD getCompressMethod() { return compressMethod_; }
	void setCompressMethod(COMPRESS_METHOD method) { compressMethod_ = method; }
private:
	bool writeMetaValues(BufferWriter<DataOutputStreamSP> &output, const ConstantSP& target, bool includeMeta,
		size_t offset, bool blocking, IO_ERR& ret);
	INDEX nextStart_{0};
	int partial_{0};
	ConstantMarshallSP marshall_;
	SymbolBaseMarshallSP symbaseMarshall_;
	COMPRESS_METHOD compressMethod_;
};

class EXPORT_DECL MatrixMarshall: public ConstantMarshallImp{
public:
	explicit MatrixMarshall(const DataOutputStreamSP& out) : ConstantMarshallImp(out),  vectorMarshall_(out){}
	~MatrixMarshall() override = default;
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	void reset() override;
private:
	bool sendMeta(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, IO_ERR& ret);


	bool rowLabelSent_{false};
	bool columnLabelSent_{false};
	bool inProgress_{false};
	VectorMarshall vectorMarshall_;
};

class EXPORT_DECL TableMarshall: public ConstantMarshallImp{
public:
	explicit TableMarshall(const DataOutputStreamSP& out) : ConstantMarshallImp(out) , vectorMarshall_(out){}
	~TableMarshall() override = default;
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	void reset() override;
private:
	bool sendMeta(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret);


	int columnNamesSent_{0};
	int nextColumn_{0};
	bool columnInProgress_{false};
	VectorMarshall vectorMarshall_;
};

class EXPORT_DECL SetMarshall: public ConstantMarshallImp{
public:
	explicit SetMarshall(const DataOutputStreamSP& out):ConstantMarshallImp(out), vectorMarshall_(out){}
	~SetMarshall() override = default;
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	void reset() override;

private:
	bool sendMeta(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, IO_ERR& ret);


	VectorMarshall vectorMarshall_;
};

class EXPORT_DECL DictionaryMarshall: public ConstantMarshallImp{
public:
	explicit DictionaryMarshall(const DataOutputStreamSP& out):ConstantMarshallImp(out),  vectorMarshall_(out){}
	~DictionaryMarshall() override = default;
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	void reset() override;
private:
	bool sendMeta(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, IO_ERR& ret);


	bool keySent_{false};
	bool inProgress_{false};
	VectorMarshall vectorMarshall_;
};

class EXPORT_DECL ChunkMarshall: public ConstantMarshallImp{
public:
	explicit ChunkMarshall(const DataOutputStreamSP& out):ConstantMarshallImp(out){}
	~ChunkMarshall() override = default;
	bool start(const char* requestHeader, size_t headerSize, const ConstantSP& target, bool blocking, bool compress, IO_ERR& ret) override;
	void reset() override;
};

class EXPORT_DECL ScalarUnmarshall: public ConstantUnmarshallImp{
public:
	explicit ScalarUnmarshall(const DataInputStreamSP& in):ConstantUnmarshallImp(in) {}
	~ScalarUnmarshall() override = default;
	bool start(uint16_t flag, bool blocking, IO_ERR& ret) override;
	void reset() override;

private:
	bool isCodeObject_{false};
	int scale_{0};
	char functionType_{-1};
};

class EXPORT_DECL SymbolBaseUnmarshall {
public:
	explicit SymbolBaseUnmarshall(const DataInputStreamSP& in): in_(in){}
	~SymbolBaseUnmarshall()= default;
	bool start(bool blocking, IO_ERR& ret);
	void reset();
	SymbolBaseSP getSymbolBase() const {
		return obj_;
	}

private:
	int symbaseId_{0};
	int size_{0};
	DataInputStreamSP in_;
	SymbolBaseSP obj_;
	std::unordered_map<int, SymbolBaseSP> dict_;
};

class EXPORT_DECL VectorUnmarshall: public ConstantUnmarshallImp{
public:
	explicit VectorUnmarshall(const DataInputStreamSP& in):ConstantUnmarshallImp(in),  unmarshall_(nullptr) {}
	~VectorUnmarshall() override = default;
	bool start(uint16_t flag, bool blocking, IO_ERR& ret) override;
	void reset() override;
	void resetSymbolBaseUnmarshall(const DataInputStreamSP& in, bool createIfNotExist);
private:
	short flag_{0};
	int rows_{0};
	int columns_{0};
	INDEX nextStart_{0};
	ConstantUnmarshallSP unmarshall_;
	SymbolBaseUnmarshallSP symbaseUnmarshall_;
	int scale_{0};
};

class EXPORT_DECL MatrixUnmarshall: public ConstantUnmarshallImp{
public:
	explicit MatrixUnmarshall(const DataInputStreamSP& in):ConstantUnmarshallImp(in),  vectorUnmarshall_(in){}
	~MatrixUnmarshall() override = default;
	bool start(uint16_t flag, bool blocking, IO_ERR& ret) override;
	void reset() override;
private:
	char labelFlag_{-1};
	bool rowLabelReceived_{false};
	bool columnLabelReceived_{false};
	bool inProgress_{false};
	ConstantSP rowLabel_;
	ConstantSP columnLabel_;
	VectorUnmarshall vectorUnmarshall_;
};

class EXPORT_DECL TableUnmarshall: public ConstantUnmarshallImp{
public:
	explicit TableUnmarshall(const DataInputStreamSP& in):ConstantUnmarshallImp(in),  vectorUnmarshall_(in){}
	~TableUnmarshall() override = default;
	bool start(uint16_t flag, bool blocking, IO_ERR& ret) override;
	void reset() override;
private:
	TABLE_TYPE type_{BASICTBL};
	bool tableNameReceived_{false};
	bool columnNameReceived_{false};
	int nextColumn_{0};
	bool inProgress_{false};
	int rows_{0};
	int columns_{0};
	std::string tableName_;
	std::vector<std::string> colNames_;
	std::vector<ConstantSP> colObjs_;
	VectorUnmarshall vectorUnmarshall_;
};

class EXPORT_DECL SetUnmarshall: public ConstantUnmarshallImp{
public:
	explicit SetUnmarshall(const DataInputStreamSP& in):ConstantUnmarshallImp(in),  vectorUnmarshall_(in) {}
	~SetUnmarshall() override = default;
	bool start(uint16_t flag, bool blocking, IO_ERR& ret) override;
	void reset() override;
private:
	bool inProgress_{false};
	VectorUnmarshall vectorUnmarshall_;
};

class EXPORT_DECL DictionaryUnmarshall: public ConstantUnmarshallImp{
public:
	explicit DictionaryUnmarshall(const DataInputStreamSP& in):ConstantUnmarshallImp(in),  vectorUnmarshall_(in) {}
	~DictionaryUnmarshall() override = default;
	bool start(uint16_t flag, bool blocking, IO_ERR& ret) override;
	void reset() override;
private:
	bool keyReceived_{false};
	ConstantSP keyVector_;
	bool inProgress_{false};
	VectorUnmarshall vectorUnmarshall_;
};

class EXPORT_DECL ChunkUnmarshall: public ConstantUnmarshallImp{
public:
	explicit ChunkUnmarshall(const DataInputStreamSP& in):ConstantUnmarshallImp(in) {}
	~ChunkUnmarshall() override = default;
	bool start(uint16_t flag, bool blocking, IO_ERR& ret) override;
	void reset() override;

private:
	IO_ERR parsing(const char* buf);

	short size_{-1};
};

class EXPORT_DECL ConstantMarshallFactory{
public:
	explicit ConstantMarshallFactory(const DataOutputStreamSP& out);
	~ConstantMarshallFactory();
	ConstantMarshall* getConstantMarshall(DATA_FORM form){return (form<0 || form>DF_CHUNK) ? nullptr: arrMarshall[form];}
	static ConstantMarshallSP getInstance(DATA_FORM form, const DataOutputStreamSP& out);

private:
	ConstantMarshall* arrMarshall[9];
};

class EXPORT_DECL ConstantUnmarshallFactory{
public:
	explicit ConstantUnmarshallFactory(const DataInputStreamSP& in);
	~ConstantUnmarshallFactory();
	ConstantUnmarshall* getConstantUnmarshall(DATA_FORM form){return (form<0 || form>DF_CHUNK) ? nullptr: arrUnmarshall[form];}
	static ConstantUnmarshallSP getInstance(DATA_FORM form, const DataInputStreamSP& in);

private:
	ConstantUnmarshall* arrUnmarshall[9];
};

} // namespace dolphindb

#ifdef _MSC_VER
#pragma warning( pop )
#endif
