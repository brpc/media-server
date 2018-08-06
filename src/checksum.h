// Media Server - Host or proxy live multimedia streams.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Ge,Jun (jge666@gmail.com)
//          Jiashun Zhu(zhujiashun2010@gmail.com)

#ifndef MEDIA_SERVER_CHECKSUM_H
#define MEDIA_SERVER_CHECKSUM_H

#include <butil/iobuf.h>
#include <ostream>
#include <functional>

struct Checksum {
    uint32_t data[4];
    
    void from_string(const std::string&);
    void from_iobuf(const butil::IOBuf&);
};

inline bool operator==(const Checksum& c1, const Checksum& c2) {
    return memcmp(c1.data, c2.data, sizeof(Checksum)) == 0;
}

inline bool operator!=(const Checksum& c1, const Checksum& c2) {
    return !(c1 == c2);
}

inline std::ostream& operator<<(std::ostream& os, const Checksum& c) {
    std::ios::fmtflags f(os.flags());
    os << std::hex << c.data[0] << c.data[1] << c.data[2] << c.data[3];
    os.flags(f);
    return os;
}

namespace BUTIL_HASH_NAMESPACE {
template <>
struct hash<Checksum> {
    std::size_t operator()(const Checksum& c) const {
        std::size_t res = c.data[2] + c.data[3];
        return (res << 32) | (c.data[0] + c.data[1]);
    }
};
}

#endif // MEDIA_SERVER_CHECKSUM_H
