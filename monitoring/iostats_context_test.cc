// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/iostats_context.h"

#include "monitoring/iostats_context_imp.h"

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

TEST(IOStatsContextTest, ToString) {
  get_iostats_context()->Reset();
  get_iostats_context()->bytes_read = 12345;

  std::string zero_included = get_iostats_context()->ToString();
  ASSERT_NE(std::string::npos, zero_included.find("= 0"));
  ASSERT_NE(std::string::npos, zero_included.find("= 12345"));

  std::string zero_excluded = get_iostats_context()->ToString(true);
  ASSERT_EQ(std::string::npos, zero_excluded.find("= 0"));
  ASSERT_NE(std::string::npos, zero_excluded.find("= 12345"));
}

TEST(IOStatsContextTest, IO_CNT) {
  auto func = [](int io_cnt) {
    IO_CNT_INC(1);
    ASSERT_EQ(io_cnt+1, GET_IO_DEPTH);
    std::cout << "IOCnt: " << GET_IO_DEPTH << "\n";
  };

  auto func_nesting = [&](int io_cnt) {
    IO_CNT_INC(1);
    ASSERT_EQ(io_cnt+1, GET_IO_DEPTH);
    func(GET_IO_DEPTH);
    std::cout << "IOCnt: " << GET_IO_DEPTH << "\n";
  };
  int io_cnt = 10;

  IO_CNT_INC(io_cnt);
  func(GET_IO_DEPTH);
  ASSERT_EQ(io_cnt, GET_IO_DEPTH);
  func_nesting(GET_IO_DEPTH);
  ASSERT_EQ(io_cnt, GET_IO_DEPTH);
  std::cout << "IOCnt: " << GET_IO_DEPTH << "\n";

};


}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
