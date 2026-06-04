// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Exports.h"
#include <cstdint>
#include <cstring>
#include <string>

namespace dolphindb {

uint32_t murmur32_16b(const unsigned char* key);
uint32_t murmur32(const char *key, size_t len);

class EXPORT_DECL Guid {
public:
    explicit Guid(bool newGuid = false);
    explicit Guid(unsigned char* guid);
    explicit Guid(const std::string& guid);
    Guid(const Guid& copy);
    Guid(unsigned long long high, unsigned long long low){
#ifndef BIGENDIANNESS
        memcpy((char*)uuid_, (char*)&low, 8);
        memcpy((char*)uuid_ + 8, (char*)&high, 8);
#else
        memcpy((char*)uuid_, (char*)&high, 8);
        memcpy((char*)uuid_ + 8, (char*)&low, 8);
#endif
    }

    Guid& operator=(const Guid& copy){
        if(this != &copy){
            memcpy(uuid_, copy.uuid_, 16);
        }
        return *this;
    }

    bool operator==(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
        return (*(long long*)a) == (*(long long*)b) && (*(long long*)(a+8)) == (*(long long*)(b+8));
    }
    bool operator!=(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
        return (*(long long*)a) != (*(long long*)b) || (*(long long*)(a+8)) != (*(long long*)(b+8));
    }
    bool operator<(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) < (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) < (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)));
#endif
    }
    bool operator>(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) > (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) > (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)));
#endif
    }
    bool operator<=(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) <= (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) < (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) <= (*(unsigned long long*)(b+8)));
#endif
    }
    bool operator>=(const Guid &other) const {
        const auto* a = (const unsigned char*)uuid_;
        const auto* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) >= (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) > (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) >= (*(unsigned long long*)(b+8)));
#endif
    }
    int compare(const Guid &other) const { return (*this < other) ? -1 : (*this > other ? 1 : 0);}
    unsigned char operator[](int i) const { return uuid_[i];}
    bool isZero() const;
    bool isNull() const {
        const auto* a = (const unsigned char*)uuid_;
        return (*(long long*)a) == 0 && (*(long long*)(a+8)) == 0;
    }
    bool isValid() const {
        const auto* a = (const unsigned char*)uuid_;
        return (*(long long*)a) != 0 || (*(long long*)(a+8)) != 0;
    }
    std::string getString() const;
    const unsigned char* bytes() const { return uuid_;}
    static std::string getString(const unsigned char* guid);

private:
    unsigned char uuid_[16];
};

struct GuidHash {
	uint64_t operator()(const Guid& guid) const;
};


} // namespace dolphindb

namespace std {
template<>
struct hash<dolphindb::Guid> {
    size_t operator()(const dolphindb::Guid& val) const{
        return dolphindb::murmur32_16b(val.bytes());
    }
};

} // namespace std
