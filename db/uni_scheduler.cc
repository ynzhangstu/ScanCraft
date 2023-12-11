#include "db/uni_scheduler.h"

#include "monitoring/iostats_context_imp.h"
#include "options/db_options.h"
#include "logging/logging.h"


namespace ROCKSDB_NAMESPACE {

void ROCKSDB_NAMESPACE::UniScheduler::SubmitIOCtx(SBCIOContex *io_ctx) {
  io_ctx->io_state_ = kSubmitted;
  io_ctx->submit_time_ = db_options_.clock->NowMicros();
  io_ctx->buffer_cv_ = &worker_cv_;

  // 考虑并发问题就采用消费者生产者模型
  io_ctx_list_wait_.push(io_ctx);
  worker_cv_.Signal();
}

bool compareSBCIOContexGreater(const SBCIOContex *a, const SBCIOContex *b) {
  return a->priority_ > b->priority_;
};

bool compareSBCIOContexCancel(const SBCIOContex *a, const SBCIOContex *b) {
  return a->priority_ < b->priority_;
};

void aio_completion_handler(int signo, siginfo_t *info, void *context) {
    struct SBCIOContex *aio = (struct SBCIOContex *)info->si_value.sival_ptr;
    auto aio_cb = aio->cb_ ;
    if (aio_error(aio_cb) == 0) {
      aio->io_state_ = kDone;
      printf("AIO operation completed successfully.\n");
      aio->buffer_cv_->Signal();
    } else {
      aio->io_state_ = kError;
      printf("AIO operation failed.\n");
    }
}

void UniScheduler::WorkerFunction() {
  ROCKS_LOG_INFO(db_options_.info_log,
                 "UniScheduler scheduler start");
  Status s;
  uint64_t wake_up_cnt = 0;
  uint64_t batch_io_cnt = 0;
  uint64_t cancel_cnt = 0;

  auto SBC_worker_start = db_options_.clock->NowMicros();

  // 工作流程如下：
  // 1. 从等待队列把数据拿到工作队列
  // 2. 对工作队列中的每个数据项算优先级
  // 3. 打包一批请求，用AIO发送IO请求
  // 4. 发出去的请求放到等待IO队列
  // 5. 发完了一批回到1
  while (running) {
    bool wait = true;

    // 从等待队列把数据拿到工作队列
    while (io_ctx_list_wait_.get_length()) {
      SBCIOContex *top;
      io_ctx_list_wait_.pop(top);
      io_ctx_list_work_.emplace_back(top);
      wait = false;
    }

    // 检查每个IO请求是否完成
    for (int64_t i = io_ctx_submitted_.size() - 1; i >= 0 ; i--) {
      wait = false;
      // auto ret = io_ctx_submitted_[i]->io_state_;
      auto ret = aio_return(io_ctx_submitted_[i]->cb_);
      if(ret > 0) {
        // 请求完成了
        ROCKS_LOG_INFO(db_options_.info_log,
                 "Finished AIO operation in UniScheduler: %" PRIu64
                 " , size: %" PRIu64 ", IO depth: %" PRIi32 "\n",
                io_ctx_submitted_[i]->file_number_, ret, GET_IO_DEPTH);
        IO_CNT_SUB(1);
        io_ctx_submitted_[i]->io_state_ = kDone;
        io_ctx_submitted_[i]->s_ = IOStatus::OK();
        io_ctx_submitted_[i]->cond_var_->Signal();
        io_ctx_submitted_.erase(io_ctx_submitted_.begin() + i);
        wait = false;
        // std::cout << "Finished AIO operation in UniScheduler: " << io_ctx_submitted_[i]->file_number_ << ", size: " << ret <<"\n";
      } else if(ret == -1) {
        printf("AIO operation failed.\n");
        io_ctx_submitted_[i]->s_ = IOStatus::IOError("AIO operation failed.");
        ROCKS_LOG_ERROR(db_options_.info_log,
                 "ERROR AIO operation in UniScheduler: %" PRIu64
                 " , size: %" PRIu64 ", IO depth: %" PRIi32 "\n",
                io_ctx_submitted_[i]->file_number_, ret, GET_IO_DEPTH);
        abort();
      }
    }

    // 检查当前IO压力是否达到上限，如果达到上限就杀请求
    if(GET_IO_DEPTH > MAX_SCHEDULE_IO_DEPTH) {
      wait = false;
      for (size_t i = io_ctx_submitted_.size() - 1; io_ctx_submitted_.size() && GET_IO_DEPTH > MAX_SCHEDULE_IO_DEPTH && i > 0; i--) {
        int ret = aio_cancel(io_ctx_submitted_[i]->cb_->aio_fildes, io_ctx_submitted_[i]->cb_);
        if (ret == AIO_CANCELED) {
          // 取消成功，把请求放回worker队列
          io_ctx_list_work_.emplace_back(std::move(io_ctx_submitted_[i]));
          io_ctx_submitted_.erase(io_ctx_submitted_.begin() + i);
          cancel_cnt++;
          ROCKS_LOG_INFO(db_options_.info_log,
                 "Cancel aio request, filenumber: %" PRIu64
                 " , IO depth now: %" PRIu32 "",
                io_ctx_submitted_[i]->file_number_, GET_IO_DEPTH);
          IO_CNT_SUB(1);
        } else if (ret == -1) {
          perror("aio_error");
          io_ctx_submitted_[i]->s_ = IOStatus::IOError("Error in cancel");
          ROCKS_LOG_ERROR(db_options_.info_log,
                 "ERROR AIO operation in UniScheduler: %" PRIu64
                 " , size: %" PRIi32 ", IO depth: %" PRIi32 "\n",
                io_ctx_submitted_[i]->file_number_, ret, GET_IO_DEPTH);
          abort();
        }
      }
    }

    if (wait && 
      io_ctx_submitted_.size() == 0 && 
      io_ctx_list_wait_.get_length() == 0 && 
      io_ctx_list_work_.size() == 0) {
      worker_cv_.Wait();
      wake_up_cnt++;
      continue;
    }

    // 打包一批请求，用AIO发送IO请求
    uint64_t batch_size = std::min(MAX_SCHEDULE_BATCH_SIZE, MAX_SCHEDULE_IO_DEPTH - GET_IO_DEPTH);
    batch_size = std::min(batch_size, io_ctx_list_work_.size());

    if (batch_size) {
      // 对工作队列中的每个数据项算优先级
      auto now = db_options_.clock->NowMicros();
      for (auto &&ctx : io_ctx_list_work_) {
        ctx->priority_ = (1 + now - ctx->submit_time_)/(ctx->weight_ * ctx->data_.size());
      }
      std::sort(io_ctx_list_work_.begin(), io_ctx_list_work_.end(), compareSBCIOContexGreater);
    
      struct aiocb* aio_array[MAX_SCHEDULE_BATCH_SIZE + 1];
      for (size_t i = 0; i < batch_size; i++) {
        auto cb = new aiocb;

        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = io_ctx_list_work_[i]->fd_;
        cb->aio_offset = io_ctx_list_work_[i]->offset_;
        cb->aio_buf = (volatile void*)io_ctx_list_work_[i]->data_.data_;
        cb->aio_nbytes = io_ctx_list_work_[i]->data_.size();
        cb->aio_lio_opcode = io_ctx_list_work_[i]->op_code_;

        // cb->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
        // cb->aio_sigevent.sigev_signo = SIGUSR1;
        // cb->aio_sigevent.sigev_value.sival_ptr = io_ctx_list_work_[i];

        io_ctx_list_work_[i]->cb_ = cb;
        aio_array[i] = cb;
      }

      IO_CNT_INC(batch_size);
      batch_io_cnt++;
      if (lio_listio(LIO_NOWAIT, aio_array, batch_size, nullptr) == -1) {
        perror("lio_listio");
        ROCKS_LOG_INFO(db_options_.info_log,
                  "UniScheduler scheduler lio_listio err");
        for (size_t i = 0; i < batch_size; i++) {
          io_ctx_list_work_[i]->s_ = IOStatus::IOError("lio_listio err");
        }
        abort();
      }
      // 把提交成功的IOCTX移到等待队列
      std::move(io_ctx_list_work_.begin(), io_ctx_list_work_.begin() + batch_size, 
        std::back_inserter(io_ctx_submitted_));
      io_ctx_list_work_.erase(io_ctx_list_work_.begin(), io_ctx_list_work_.begin() + batch_size);
    }
  }
  auto SBC_worker_end = db_options_.clock->NowMicros();

  ROCKS_LOG_INFO(db_options_.info_log,
                 "UniScheduler scheduler stopped, Wake up times: %" PRIu64
                 " Batch IO num: %" PRIu64 " Cancel num: %" PRIu64 " Duration :%" PRIu64 " ms",
                wake_up_cnt, batch_io_cnt, 
                 cancel_cnt, SBC_worker_end - SBC_worker_start);
}

}  // namespace ROCKSDB_NAMESPACE
