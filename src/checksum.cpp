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

#include "checksum.h"
#include "butil/third_party/murmurhash3/murmurhash3.h"

void Checksum::from_string(const std::string& str) {
    butil::MurmurHash3_x64_128_Context ctx;
    butil::MurmurHash3_x64_128_Init(&ctx, 0);
    butil::MurmurHash3_x64_128_Update(&ctx, str.data(), str.size());
    butil::MurmurHash3_x64_128_Final(data, &ctx);
}

void Checksum::from_iobuf(const butil::IOBuf& buf) {
    butil::MurmurHash3_x64_128_Context ctx;
    butil::MurmurHash3_x64_128_Init(&ctx, 0);
    for (size_t i = 0; i < buf.backing_block_num(); ++i) {
        butil::StringPiece blk = buf.backing_block(i);
        butil::MurmurHash3_x64_128_Update(&ctx, blk.data(), blk.size());
    }
    butil::MurmurHash3_x64_128_Final(data, &ctx);
}

