#pragma once

#include <cinttypes>
#include <vector>
#include "sys/time.h"
#include <aio.h>
#include <mutex>
#include <signal.h>

#include "rocksdb/db.h"
#include "db/dbformat.h"
#include "memory/arena.h"
#include "util/lock_free_queue.h"
#include "monitoring/instrumented_mutex.h"

namespace ROCKSDB_NAMESPACE
{
class CompactionJob;

#define MAX_SCHEDULE_BATCH_SIZE 4ll
#define MAX_SCHEDULE_IO_DEPTH   32ll
#define PAUSE() asm("pause\n")

struct KeyValueNode {
  char *key;
  char *value;
  ParsedInternalKey ikey;
  size_t key_size;
  size_t value_size;
  Status input_status;
};

class SBCKeyValueBuffer {
 public:

  SBCKeyValueBuffer(LockFreeQueue<KeyValueNode> *queue):queue_(queue), from_uni_scheduler_(true) {}

  SBCKeyValueBuffer(size_t size):from_uni_scheduler_(false){
    queue_ = new LockFreeQueue<KeyValueNode>(size);

  }

  ~SBCKeyValueBuffer() {
    if(!from_uni_scheduler_) {
      delete queue_;
    }
  }

  Status AddKeyValue(const Slice &key, const Slice &value, const ParsedInternalKey &ikey) {
    size_t alloc_size = key.size() + value.size() + sizeof(KeyValueNode);
    auto addr = allocator_.Allocate(alloc_size);
    if(addr == nullptr) {
      return Status::MemoryLimit("AddKeyValue alloc memory failed");
    }

    char* key_addr = addr;
    char* value_addr = addr + key.size();
    KeyValueNode* kv_node = new (addr + key.size() + value.size()) KeyValueNode;

    memcpy(key_addr, key.data(), key.size());
    memcpy(value_addr, value.data(), value.size());

    kv_node->key = key_addr;
    kv_node->value = value_addr;

    kv_node->ikey.sequence = ikey.sequence;
    kv_node->ikey.type = ikey.type;
    kv_node->ikey.user_key = Slice(key_addr, key.size() - kNumInternalBytes);

    kv_node->key_size = key.size();
    kv_node->value_size = value.size();
    kv_node->input_status = Status::OK();

    bool s = false;
    s = queue_->push(kv_node);
    if(s) {
      return Status::OK();
    } else {
      return Status::NoSpace("Key value queue full");
    }
  }

  // NOTE: 手动释放Node内存
  KeyValueNode* PopKeyValue() {
    KeyValueNode *t;
    if (queue_->pop(t)) {
      return t;
    } else {
      return nullptr;
    }
    
  }

  size_t size() {
    return queue_->get_length();
  }

 private:
  LockFreeQueue<KeyValueNode> *queue_;
  Arena allocator_;
  bool from_uni_scheduler_;
};


enum SBCIOState : uint32_t {
  kInit,
  kSubmitted,
  kDone,
  kError
};

struct SBCIOContex{
  int fd_ = -1;                        // For AIO
  Slice data_;                         // For AIO
  uint64_t offset_;                    // For AIO

  size_t file_advance_;                // From WritableFileWriter
  size_t leftover_tail_;               // From WritableFileWriter

  IOStatus s_;                         // From UniScheduler
  double priority_;                    // From UniScheduler
  uint64_t submit_time_;               // From UniScheduler
  struct aiocb *cb_ = nullptr;         // From UniScheduler
  SBCIOState io_state_ = kInit;        // From UniScheduler
  port::CondVar *buffer_cv_ = nullptr; // From UniScheduler

  int op_code_;                        // From Submitter
  uint64_t weight_;                    // From Submitter
  uint64_t file_number_;               // From Submitter
  InstrumentedCondVar *cond_var_;      // From Submitter
};

void aio_completion_handler(int signo, siginfo_t *info, void *context);

class UniScheduler
{
public:
  // UniScheduler() = default;
  explicit UniScheduler(uint64_t buffer_size, const ImmutableDBOptions& db_options):
    left_size_(buffer_size), db_options_(db_options), queue_(1024*1024), 
    io_ctx_list_wait_(1024), work_mu_(), worker_cv_(&work_mu_), running(false) {
    io_ctx_list_work_.reserve(1000);
    io_ctx_submitted_.reserve(100);

    // struct sigaction sa;
    // sa.sa_flags = SA_SIGINFO;
    // sa.sa_sigaction = aio_completion_handler;
    // sigaction(SIGUSR1, &sa, NULL);

    running = true;
    auto func = [this]() {
      this->WorkerFunction();
    };
    worker = std::thread(func);
  }

  ~UniScheduler() {
    running = false;
    worker_cv_.Signal();
    worker.join();
  }

  UniScheduler(UniScheduler&&) = delete;
  UniScheduler(const UniScheduler&) = delete;
  UniScheduler& operator=(const UniScheduler&) = delete;

  SBCKeyValueBuffer* GetSBCKeyValueBuffer() {
    return new SBCKeyValueBuffer(&queue_);
  }

  void SubmitIOCtx(SBCIOContex *io_ctx);

  void WorkerFunction();

private:
  uint64_t left_size_;
  const ImmutableDBOptions& db_options_;
  std::vector<CompactionJob*> sbc_job_list_;
  LockFreeQueue<KeyValueNode> queue_;
  LockFreeQueue<SBCIOContex> io_ctx_list_wait_;
  std::vector<SBCIOContex*> io_ctx_list_work_;
  std::vector<SBCIOContex*> io_ctx_submitted_;
  port::Mutex work_mu_;
  port::CondVar worker_cv_;
  std::thread worker;    // NOTE: 由它负载把请求打包
  std::atomic<bool> running;
};


} // namespace ROCKSDB_NAMESPACE
