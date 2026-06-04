// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Types.h"
#include "Exports.h"
#include <exception>
#include <string>
#include <utility>

#ifdef _MSC_VER
#define DDB_FUNCNAME __FUNCSIG__
#else
#define DDB_FUNCNAME __PRETTY_FUNCTION__
#endif

namespace dolphindb {

class IncompatibleTypeException: public std::exception{
public:
	IncompatibleTypeException(DATA_TYPE expected, DATA_TYPE actual)
		: expected_(expected), actual_(actual)
	{
		errMsg_.append("Incompatible type. Expected: " + getDataTypeName(expected_) + ", Actual: " + getDataTypeName(actual_));
	}

	~IncompatibleTypeException() noexcept override = default;
	const char* what() const noexcept override { return errMsg_.c_str();}
	DATA_TYPE expectedType(){return expected_;}
	DATA_TYPE actualType(){return actual_;}
private:
	DATA_TYPE expected_;
	DATA_TYPE actual_;
	std::string errMsg_;
};

class IllegalArgumentException : public std::exception{
public:
	IllegalArgumentException(std::string functionName, std::string errMsg): functionName_(std::move(functionName)), errMsg_(std::move(errMsg)){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~IllegalArgumentException() noexcept override = default;
	const std::string& getFunctionName() const { return functionName_;}

private:
	const std::string functionName_;
	const std::string errMsg_;
};

class RuntimeException: public std::exception{
public:
	explicit RuntimeException(std::string errMsg):errMsg_(std::move(errMsg)){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~RuntimeException() noexcept override = default;

private:
	const std::string errMsg_;
};

class OperatorRuntimeException: public std::exception{
public:
	OperatorRuntimeException(std::string optr,std::string errMsg): operator_(std::move(optr)),errMsg_(std::move(errMsg)){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~OperatorRuntimeException() noexcept override = default;
	const std::string& getOperatorName() const { return operator_;}

private:
	const std::string operator_;
	const std::string errMsg_;
};

class TableRuntimeException: public std::exception{
public:
	explicit TableRuntimeException(std::string errMsg): errMsg_(std::move(errMsg)){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~TableRuntimeException() noexcept override = default;

private:
	const std::string errMsg_;
};

class MemoryException: public std::exception{
public:
	MemoryException():errMsg_("Out of memory"){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~MemoryException() noexcept override = default;

private:
	const std::string errMsg_;
};

class IOException: public std::exception{
public:
	explicit IOException(std::string errMsg): errMsg_(std::move(errMsg)), errCode_(OTHERERR){}
	IOException(const std::string& errMsg, IO_ERR errCode): errMsg_(errMsg + ". " + getCodeDescription(errCode)), errCode_(errCode){}
	explicit IOException(IO_ERR errCode): errMsg_(getCodeDescription(errCode)), errCode_(errCode){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~IOException() noexcept override = default;
	IO_ERR getErrorCode() const {return errCode_;}
private:
    std::string getCodeDescription(IO_ERR errCode) const
    {
        switch (errCode) {
        case OK: return "";
        case DISCONNECTED: return "Socket is disconnected/closed or file is closed.";
        case NODATA: return "In non-blocking socket mode, there is no data ready for retrieval yet.";
        case NOSPACE: return "Out of memory, no disk space, or no buffer for sending data in non-blocking socket mode.";
        case TOO_LARGE_DATA: return "String size exceeds 64K or code size exceeds 1 MB during serialization over network.";
        case INPROGRESS: return "In non-blocking socket mode, a program is in pending connection mode.";
        case INVALIDDATA: return "Invalid message format";
        case END_OF_STREAM: return "Reach the end of a file or a buffer.";
        case READONLY: return "File is readable but not writable.";
        case WRITEONLY: return "File is writable but not readable.";
        case NOTEXIST: return "A file doesn't exist or the socket destination is not reachable.";
        case OTHERERR: return "Unknown IO error.";
        default: return "";
        }
    }


	const std::string errMsg_;
	const IO_ERR errCode_;
};

class DataCorruptionException: public std::exception {
public:
	explicit DataCorruptionException(const std::string& errMsg) : errMsg_("<DataCorruption>" + errMsg){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~DataCorruptionException() noexcept override = default;

private:
	const std::string errMsg_;
};

class NotLeaderException: public std::exception {
public:
	//Electing a leader. Wait for a while to retry.
	NotLeaderException() : errMsg_("<NotLeader>"){}
	//Use the new leader specified in the input argument. format: <host>:<port>:<alias>, e.g. 192.168.1.10:8801:nodeA
	explicit NotLeaderException(const std::string& newLeader) : errMsg_("<NotLeader>" + newLeader), newLeader_(newLeader){}
	const std::string& getNewLeader() const {return newLeader_;}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~NotLeaderException() noexcept override = default;

private:
	const std::string errMsg_;
	const std::string newLeader_;
};

class MathException: public std::exception {
public:
	explicit MathException(std::string errMsg) : errMsg_(std::move(errMsg)){}
	const char* what() const noexcept override{
		return errMsg_.c_str();
	}
	~MathException() noexcept override = default;

private:
	const std::string errMsg_;
};

} // namespace dolphindb
