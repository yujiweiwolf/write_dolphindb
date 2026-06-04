// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Exports.h"
#include <cfloat>
#include <climits>
#include <string>

namespace dolphindb {

// CONSTANT_xxx are legacy macros. All these except CONSTANT_DOUBLE_ENUM are replaced by enum DATA_TYPE.
#define CONSTANT_VOID 0
#define CONSTANT_BOOL 1
#define CONSTANT_CHAR 2
#define CONSTANT_SHORT 3
#define CONSTANT_INT 4
#define CONSTANT_LONG 5
#define CONSTANT_DATE 6
#define CONSTANT_MONTH 7
#define CONSTANT_TIME 8
#define CONSTANT_MINUTE 9
#define CONSTANT_SECOND 10
#define CONSTANT_DATETIME 11
#define CONSTANT_TIMESTAMP 12
#define CONSTANT_NANOTIME 13
#define CONSTANT_NANOTIMESTAMP 14
#define CONSTANT_FLOAT 15
#define CONSTANT_DOUBLE 16
#define CONSTANT_SYMBOL 17
#define CONSTANT_STRING 18
#define CONSTANT_UUID 19

#define CONSTANT_DOUBLE_ENUM 20

#define CONSTANT_DATEHOUR 28
#define CONSTANT_DATEMINUTE 29
#define CONSTANT_IP 30
#define CONSTANT_INT128 31

const double MC_PI = 3.1415926535897932384626433832795028841971693994L;
const double MC_E = 2.7182818284590452353602874713526624977572470937L;
const double DBL_ONE_LIMIT=0.9999999999999;
const float FLT_NMIN=-FLT_MAX;
const double DBL_NMIN=-DBL_MAX;
const int MAX_SHARED_OBJ_INDEX = 65536;
const int MAX_FREEABLE_OBJ_COUNT = 4096;
const size_t MAX_ARRAY_BUFFER = 1677721600;
const int MAX_PEICE_FOR_PEICEWISE = 16;
const int MAX_POLYNOMIAL_ORDER = 128;
const int MAX_ROWS_FOR_MATRIX_COMPUTING = 8192;
const std::string functionKeyword = "def";
const std::string aggregationKeyword = "defg";
const std::string mapreduceKeyword = "mapr";

constexpr int ARRAY_TYPE_BASE = 64;

enum DATA_TYPE {
    DT_VOID = 0,
    DT_BOOL = 1,
    DT_CHAR = 2,
    DT_SHORT = 3,
    DT_INT = 4,
    DT_LONG = 5,
    DT_DATE = 6,
    DT_MONTH = 7,
    DT_TIME = 8,
    DT_MINUTE = 9,
    DT_SECOND = 10,
    DT_DATETIME = 11,
    DT_TIMESTAMP = 12,
    DT_NANOTIME = 13,
    DT_NANOTIMESTAMP = 14,
    DT_FLOAT = 15,
    DT_DOUBLE = 16,
    DT_SYMBOL = 17,
    DT_STRING = 18,
    DT_UUID = 19,
    DT_FUNCTIONDEF = 20,
    DT_HANDLE = 21,
    DT_CODE = 22,
    DT_DATASOURCE = 23,
    DT_RESOURCE = 24,
    DT_ANY = 25,
    DT_COMPRESS = 26,
    DT_DICTIONARY = 27,
    DT_DATEHOUR = 28,
    DT_DATEMINUTE = 29,
    DT_IP = 30,
    DT_INT128 = 31,
    DT_BLOB = 32,
    DT_DECIMAL = 33,
    DT_COMPLEX = 34,
    DT_POINT = 35,
    DT_DURATION = 36,
    DT_DECIMAL32 = 37,
    DT_DECIMAL64 = 38,
    DT_DECIMAL128 = 39,
    DT_OBJECT = 40,
    DT_IOTANY = 41,              // not supported
    DT_COUNT = 42,               // Number of Types
    DT_VOID_ARRAY = ARRAY_TYPE_BASE,
    DT_BOOL_ARRAY = 65,
    DT_CHAR_ARRAY = 66,
    DT_SHORT_ARRAY = 67,
    DT_INT_ARRAY = 68,
    DT_LONG_ARRAY = 69,
    DT_DATE_ARRAY = 70,
    DT_MONTH_ARRAY = 71,
    DT_TIME_ARRAY = 72,
    DT_MINUTE_ARRAY = 73,
    DT_SECOND_ARRAY = 74,
    DT_DATETIME_ARRAY = 75,
    DT_TIMESTAMP_ARRAY = 76,
    DT_NANOTIME_ARRAY = 77,
    DT_NANOTIMESTAMP_ARRAY = 78,
    DT_FLOAT_ARRAY = 79,
    DT_DOUBLE_ARRAY = 80,
    DT_SYMBOL_ARRAY = 81,
    DT_STRING_ARRAY = 82,
    DT_UUID_ARRAY = 83,
    DT_FUNCTIONDEF_ARRAY = 84,
    DT_HANDLE_ARRAY = 85,
    DT_CODE_ARRAY = 86,
    DT_DATASOURCE_ARRAY = 87,
    DT_RESOURCE_ARRAY = 88,
    DT_ANY_ARRAY = 89,
    DT_COMPRESS_ARRAY = 90,
    DT_DICTIONARY_ARRAY = 91,
    DT_DATEHOUR_ARRAY = 92,
    DT_DATEMINUTE_ARRAY = 93,
    DT_IP_ARRAY = 94,
    DT_INT128_ARRAY = 95,
    DT_BLOB_ARRAY = 96,
    // DT_DECIMAL_ARRAY,	not supported
    // DT_COMPLEX_ARRAY,	not supported
    // DT_POINT_ARRAY,		not supported
    // DT_DURATION_ARRAY,	not supported
    DT_DECIMAL32_ARRAY = 101,
    DT_DECIMAL64_ARRAY = 102,
    DT_DECIMAL128_ARRAY = 103,
    DT_OBJECT_ARRAY = 104,
    DT_IOTANY_ARRAY = 105,
};

EXPORT_DECL std::string getDataTypeName(DATA_TYPE type);

constexpr int TYPE_COUNT = DT_COUNT;

enum DATA_CATEGORY {
    NOTHING,
    LOGICAL,
    INTEGRAL,
    FLOATING,
    TEMPORAL,
    LITERAL,
    SYSTEM,
    MIXED,
    BINARY,
    COMPLEX,
    ARRAY,
    DENARY,
    MAX_CATEGORY
};

enum class VECTOR_TYPE {
    ARRAY = 0,
    ARRAYVECTOR = 6,
};

/**
 * The order of the enumerated DATA_FORM items matters. In Constant class, we use 4 bits rather than a enum variable to
 * indicate the data form of the constant object. In other words, the value of each DATA_FORM instance is hard coded.
 * In case we have to change the value, please review the constructor of Constant, Vector, Set, Dictionary, and Table.
 */
enum DATA_FORM {
    DF_SCALAR,
    DF_VECTOR,
    DF_PAIR,
    DF_MATRIX,
    DF_SET,
    DF_DICTIONARY,
    DF_TABLE,
    DF_CHART,
    DF_CHUNK,
    DF_SYSOBJ,
    DF_TENSOR,      // not supported
    MAX_DATA_FORM
};

enum OBJECT_TYPE { CONSTOBJ };

enum TABLE_TYPE {
    BASICTBL = 0,
    COMPRESSTBL = 8,
    IPCTBL = 15,    // this is different from server
};

enum IO_ERR {
    OK,
    DISCONNECTED,
    NODATA,
    NOSPACE,
    TOO_LARGE_DATA,
    INPROGRESS,
    INVALIDDATA,
    END_OF_STREAM,
    READONLY,
    WRITEONLY,
    NOTEXIST,
    CORRUPT,
    NOT_LEADER,
    OTHERERR
};

enum PARTITION_TYPE {
    SEQ,
    VALUE,
    RANGE,
    LIST,
    HIER,
    HASH,
    MAX_PARTITION_TYPE
};

enum CHUNK_TYPE {
    FILE_CHUNK,
    TABLET_CHUNK,
    SPLIT_TABLET_CHUNK,
    SMALLFILE_CHUNK,
    ADDITIONAL_CHUNK
};

enum DURATION_UNIT {
    DU_NANOSECOND,
    DU_MICROSECOND,
    DU_MILLISECOND,
    DU_SECOND,
    DU_MINUTE,
    DU_HOUR,
    DU_DAY,
    DU_WEEK,
    DU_MOUNTH,
    DU_YEAR
};

enum STREAM_TYPE {
    ARRAY_STREAM,
    SOCKET_STREAM,
    QUEUE_STREAM,
    FILE_STREAM,
};

enum COMPRESS_METHOD {
    COMPRESS_NONE = 0,
    COMPRESS_LZ4 = 1,
    COMPRESS_DELTA = 2,
};

#ifdef INDEX64
    typedef long long INDEX;
    typedef unsigned long long UINDEX;
    #define DT_INDEX DT_LONG
    const long long INDEX_MIN = LLONG_MIN;
    const long long INDEX_MAX = LLONG_MAX;
#else
    using INDEX = int;
    using UINDEX = unsigned int;
    #define DT_INDEX DT_INT
    const int INDEX_MIN = INT_MIN;
    const int INDEX_MAX = INT_MAX;
#endif

union U8 {
    long long longVal;
    int intVal;
    short shortVal;
    char charVal;
    double doubleVal;
    float floatVal;
    char* pointer;
};

} // namespace dolphindb
