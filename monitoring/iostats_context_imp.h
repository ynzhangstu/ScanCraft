//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/iostats_context.h"

#include <atomic>

#if !defined(NIOSTATS_CONTEXT)
namespace ROCKSDB_NAMESPACE {
extern thread_local IOStatsContext iostats_context;


class GlobalIOStat {
public:
  static std::atomic<int> __global_io_depth;
};


class AutoIoDepthIncrement {
public:
  AutoIoDepthIncrement(int cnt) {
    cnt_ = cnt;
    GlobalIOStat::__global_io_depth.fetch_add(cnt, std::memory_order_relaxed);
  }

  ~AutoIoDepthIncrement() {
    GlobalIOStat::__global_io_depth.fetch_sub(cnt_, std::memory_order_relaxed);
  }
  int cnt_;
};
}  // namespace ROCKSDB_NAMESPACE

#define IO_CNT_INC_GUARD(io_cnt) \
    ROCKSDB_NAMESPACE::AutoIoDepthIncrement ___autoIoDepthIncrement(io_cnt);

#define IO_CNT_INC(io_cnt) \
    ROCKSDB_NAMESPACE::GlobalIOStat::__global_io_depth.fetch_add(io_cnt, std::memory_order_relaxed);

#define IO_CNT_SUB(io_cnt) \
    ROCKSDB_NAMESPACE::GlobalIOStat::__global_io_depth.fetch_sub(io_cnt, std::memory_order_relaxed);

#define GET_IO_DEPTH \
    (ROCKSDB_NAMESPACE::GlobalIOStat::__global_io_depth.load(std::memory_order_relaxed))


// increment a specific counter by the specified value
#define IOSTATS_ADD(metric, value)        \
  if (!iostats_context.disable_iostats) { \
    iostats_context.metric += value;      \
  }

// reset a specific counter to zero
#define IOSTATS_RESET(metric) (iostats_context.metric = 0)

// reset all counters to zero
#define IOSTATS_RESET_ALL() (iostats_context.Reset())

#define IOSTATS_SET_THREAD_POOL_ID(value) \
  (iostats_context.thread_pool_id = value)

#define IOSTATS_THREAD_POOL_ID() (iostats_context.thread_pool_id)

#define IOSTATS(metric) (iostats_context.metric)

// Declare and set start time of the timer
#define IOSTATS_TIMER_GUARD(metric)                                     \
  PerfStepTimer iostats_step_timer_##metric(&(iostats_context.metric)); \
  iostats_step_timer_##metric.Start();

// Declare and set start time of the timer
#define IOSTATS_CPU_TIMER_GUARD(metric, clock)         \
  PerfStepTimer iostats_step_timer_##metric(           \
      &(iostats_context.metric), clock, true,          \
      PerfLevel::kEnableTimeAndCPUTimeExceptForMutex); \
  iostats_step_timer_##metric.Start();

#define IOSTATS_SET_DISABLE(disable) (iostats_context.disable_iostats = disable)

#else  // !NIOSTATS_CONTEXT

#define IOSTATS_ADD(metric, value)
#define IOSTATS_ADD_IF_POSITIVE(metric, value)
#define IOSTATS_RESET(metric)
#define IOSTATS_RESET_ALL()
#define IOSTATS_SET_THREAD_POOL_ID(value)
#define IOSTATS_THREAD_POOL_ID()
#define IOSTATS(metric) 0
#define IOSTATS_SET_DISABLE(disable)

#define IOSTATS_TIMER_GUARD(metric)
#define IOSTATS_CPU_TIMER_GUARD(metric, clock) static_cast<void>(clock)

#endif  // !NIOSTATS_CONTEXT
