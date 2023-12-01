#include <iostream>
#include <vector>
#include <aio.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <atomic>

#include <sys/time.h>


static int numFiles = 100;
static int bufferSize = 10 * 1024 * 1024;

namespace {

static inline int64_t GetUnixTimeUs() {
  struct timeval tp;
  gettimeofday(&tp, nullptr);
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

bool CheckArr(void *arr, char c,int size) {
    char *arr_char = (char*)arr;
    for (int i = 0; i < size; i++) {
        if (arr_char[i] != c) {
            return false;
        }
    }
    return true;
}

}

std::atomic<int> cnt_io_ = 0;

void aio_write_callback(sigval_t sigval) {
    struct aiocb *cb = (struct aiocb *)sigval.sival_ptr;
    if (aio_error(cb) == 0) {
        // std::cout << "AIO Write Completed for file " << cb->aio_fildes << std::endl;
        cnt_io_++;
    } else {
        std::cerr << "AIO Error for file " << cb->aio_fildes << std::endl;
    }
    // close(cb->aio_fildes);
    // free(const_cast<void*>(cb->aio_buf));
    // free(cb);
}

void aio_read_callback(sigval_t sigval) {
    struct aiocb *cb = (struct aiocb *)sigval.sival_ptr;
    if (aio_error(cb) == 0) {
        std::cout << "AIO read Completed for file " << cb->aio_fildes << std::endl;
        cnt_io_++;
    } else {
        std::cerr << "AIO read Error for file " << cb->aio_fildes << std::endl;
    }
    close(cb->aio_fildes);
}


int AIONoBatchWrite() {
  std::cout << "Write no batch\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    std::vector<struct aiocb*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        struct aiocb *cb = new aiocb;
        std::string fileName = fileNames[i];

        int result = std::remove(fileName.c_str());

        int fileDescriptor = open(fileName.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }
        memset(buffer, '0'+i, bufferSize);
        // snprintf(buffer, bufferSize, "Data for file %s\n", fileName.c_str());

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
        cb->aio_sigevent.sigev_notify_function = aio_write_callback;
        cb->aio_sigevent.sigev_notify_attributes = NULL;
        cb->aio_sigevent.sigev_value.sival_ptr = cb;

        aioList.push_back(cb);

    }

    auto write_start = GetUnixTimeUs();

    for (int i = 0; i < numFiles; ++i) {
        // Asynchronously write data to the file
        if (aio_write(aioList[i]) == -1) {
            perror("aio_write");
            return 1;
        }
        aio_suspend(&aioList[i], 1, nullptr);
    }

    auto submit_finished = GetUnixTimeUs();

    // Wait for all AIO operations to complete
    // for (int i = 0; i < numFiles; ++i) {
    //     aio_suspend(aioList.data(), numFiles, nullptr);
    // }
    
    auto write_finished = GetUnixTimeUs();

    sleep(1);

    std::cout << "Write:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << write_finished - write_start << "us\n"
              << "Submit dur: " << submit_finished - write_start << "us\n"
              << "IO cnt" << cnt_io_
              << "\n\n";

    // Clean up
    for (int i = 0; i < numFiles; ++i) {
        close(aioList[i]->aio_fildes);
    }
    return 0;
}


int AIOBatchWrite() {
  std::cout << "\nWrite batch\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    std::vector<struct aiocb*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        struct aiocb *cb = new aiocb;
        std::string fileName = fileNames[i];

        int result = std::remove(fileName.c_str());

        int fileDescriptor = open(fileName.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }
        memset(buffer, '0'+i, bufferSize);
        // snprintf(buffer, bufferSize, "Data for file %s\n", fileName.c_str());

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_lio_opcode = LIO_WRITE;

        cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
        cb->aio_sigevent.sigev_notify_function = aio_write_callback;
        cb->aio_sigevent.sigev_notify_attributes = NULL;
        cb->aio_sigevent.sigev_value.sival_ptr = cb;

        aioList.push_back(cb);

    }

    struct aiocb* aioArray[numFiles];
    for (int i = 0; i < numFiles; ++i) {
        aioArray[i] = aioList[i];
        // std::cout << aioArray[i]->aio_fildes << "\n";
    }

    auto write_start = GetUnixTimeUs();

    // Use lio_listio to simultaneously submit multiple aio requests
    if (lio_listio(LIO_NOWAIT, aioArray, numFiles, nullptr) == -1) {
        perror("lio_listio");
        return 1;
    }

    auto submit_finished = GetUnixTimeUs();

    for (int i = 0; i < numFiles; ++i) {
        aio_suspend(aioList.data(), numFiles, nullptr);
    }

    auto write_finished = GetUnixTimeUs();


    sleep(1);

    std::cout << "Write:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << write_finished - write_start << "us\n"
              << "Submit dur: " << submit_finished - write_start << "us\n"
              << "IO cnt: " << cnt_io_
              << "\n\n";

    // Clean up
    for (int i = 0; i < numFiles; ++i) {
        close(aioList[i]->aio_fildes);
    }
    return 0;
}


int AIONoBatchRead() {
  std::cout << "Read no batch\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    std::vector<struct aiocb*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        struct aiocb *cb = new aiocb;
        std::string fileName = fileNames[i];

        int fileDescriptor = open(fileName.c_str(), O_RDONLY | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        // char *buffer = (char *)malloc(bufferSize);
        // char *buffer = aligned_alloc(512, bufferSize);
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }

        memset(buffer, 0, bufferSize);
        // snprintf(buffer, bufferSize, "Data for file %s\n", fileName.c_str());

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
        cb->aio_sigevent.sigev_notify_function = aio_read_callback;
        cb->aio_sigevent.sigev_notify_attributes = NULL;
        cb->aio_sigevent.sigev_value.sival_ptr = cb;

        aioList.push_back(cb);

    }

    auto _start = GetUnixTimeUs();

    for (int i = 0; i < numFiles; ++i) {
        if (aio_read(aioList[i]) == -1) {
            perror("aio_read");
            return 1;
        }
        aio_suspend(&aioList[i], 1, nullptr);
    }

    auto submit_finished = GetUnixTimeUs();

    // // Wait for all AIO operations to complete
    // for (int i = 0; i < numFiles; ++i) {
    //     aio_suspend(aioList.data(), numFiles, nullptr);
    // }

    // int cacnel_num = 5;
    // for (int i = numFiles-1; i >= numFiles-cacnel_num; --i) {
    //     if (aio_cancel(aioList[i]->aio_fildes, aioList[i]) == AIO_CANCELED) {
    //         printf("Request %d was successfully canceled.\n", aioList[i]->aio_fildes);
    //     } else if (aio_error(aioList[i]) == EINPROGRESS) {
    //         printf("Request %d is still in progress.\n", aioList[i]->aio_fildes);
    //     } else {
    //         perror("aio_error");
    //         return 1;
    //     }
    // }
    
    auto _finished = GetUnixTimeUs();

    sleep(1);

    std::cout << "Read:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << _finished - _start << "us\n"
              << "Submit dur: " << submit_finished - _start << "us\n"
              << "IO cnt" << cnt_io_
              << "\n\n";

    for (int i = 0; i < numFiles; i++)
    {
      if(!CheckArr(const_cast<void*>(aioList[i]->aio_buf), '0'+i, bufferSize)) {
        std::cout << "Read error " << i << "\n";
        break;
      }
      free(const_cast<void*>(aioList[i]->aio_buf));
      free(aioList[i]);
    }  
    return 0;
}


int AIOBatchRead() {
  std::cout << "\nRead batch\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    // struct aioinit init;
    // init.aio_threads = 20;
    // init.aio_num = 64;
    // aio_init(&init);

    std::vector<struct aiocb*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        struct aiocb *cb = new aiocb;
        std::string fileName = fileNames[i];

        int fileDescriptor = open(fileName.c_str(), O_RDONLY | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }
        memset(buffer, 0, bufferSize);
        // snprintf(buffer, bufferSize, "Data for file %s\n", fileName.c_str());

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_lio_opcode = LIO_READ;

        cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
        cb->aio_sigevent.sigev_notify_function = aio_read_callback;
        cb->aio_sigevent.sigev_notify_attributes = NULL;
        cb->aio_sigevent.sigev_value.sival_ptr = cb;

        aioList.push_back(cb);

    }

    struct aiocb* aioArray[numFiles];
    for (int i = 0; i < numFiles; ++i) {
        aioArray[i] = aioList[i];
        // std::cout << aioArray[i]->aio_fildes << "\n";
    }

    auto _start = GetUnixTimeUs();

    // Use lio_listio to simultaneously submit multiple aio requests
    // LIO_WAIT可以阻塞调用这个方法
    if (lio_listio(LIO_NOWAIT, aioArray, numFiles, nullptr) == -1) {
        perror("lio_listio");
        return 1;
    }

    auto submit_finished = GetUnixTimeUs();

    for (int i = 0; i < numFiles; ++i) {
        aio_suspend(aioList.data(), numFiles, nullptr);
    }

    auto _finished = GetUnixTimeUs();

    sleep(1);

    std::cout << "Read:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << _finished - _start << "us\n"
              << "Submit dur: " << submit_finished - _start << "us\n"
              << "IO cnt: " << cnt_io_
              << "\n\n";
    for (int i = 0; i < numFiles; i++)
    {
      if(!CheckArr(const_cast<void*>(aioList[i]->aio_buf), '0'+i, bufferSize)) {
        std::cout << "Read error " << i << "\n";
        break;
      }
      free(const_cast<void*>(aioList[i]->aio_buf));
      free(aioList[i]);
    }  

    return 0;
}


int TestWriteCancel() {
  std::cout << "Write no batch cancel\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    std::vector<struct aiocb*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        struct aiocb *cb = new aiocb;
        std::string fileName = fileNames[i];

        int result = std::remove(fileName.c_str());

        int fileDescriptor = open(fileName.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }
        memset(buffer, '0'+i, bufferSize);
        // snprintf(buffer, bufferSize, "Data for file %s\n", fileName.c_str());

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
        cb->aio_sigevent.sigev_notify_function = aio_write_callback;
        cb->aio_sigevent.sigev_notify_attributes = NULL;
        cb->aio_sigevent.sigev_value.sival_ptr = cb;

        aioList.push_back(cb);

    }

    auto write_start = GetUnixTimeUs();

    for (int i = 0; i < numFiles; ++i) {
        // Asynchronously write data to the file
        if (aio_write(aioList[i]) == -1) {
            perror("aio_write");
            return 1;
        }
        // aio_suspend(&aioList[i], 1, nullptr);
        if (aio_cancel(aioList[i]->aio_fildes, aioList[i]) == AIO_CANCELED) {
            printf("Request %d was successfully canceled.\n", aioList[i]->aio_fildes);
        } else if (aio_error(aioList[i]) == EINPROGRESS) {
            printf("Request %d is still in progress.\n", aioList[i]->aio_fildes);
        } else {
            perror("aio_error");
            return 1;
        }
    }

    auto submit_finished = GetUnixTimeUs();

    // Wait for all AIO operations to complete
    for (int i = 0; i < numFiles; ++i) {
        aio_suspend(aioList.data(), numFiles, nullptr);
    }

    // int cacnel_num = 5;
    // for (int i = numFiles-1; i >= numFiles-cacnel_num; --i) {
    //     if (aio_cancel(aioList[i]->aio_fildes, aioList[i]) == AIO_CANCELED) {
    //         printf("Request %d was successfully canceled.\n", aioList[i]->aio_fildes);
    //     } else if (aio_error(aioList[i]) == EINPROGRESS) {
    //         printf("Request %d is still in progress.\n", aioList[i]->aio_fildes);
    //     } else {
    //         perror("aio_error");
    //         return 1;
    //     }
    // }
    
    auto write_finished = GetUnixTimeUs();

    sleep(1);

    std::cout << "Write:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << write_finished - write_start << "us\n"
              << "Submit dur: " << submit_finished - write_start << "us\n"
              << "IO cnt" << cnt_io_
              << "\n\n";

    // Clean up
    for (int i = 0; i < numFiles; ++i) {
        close(aioList[i]->aio_fildes);
    }
    return 0;
}


int TestWriteBatchCancel() {
  std::cout << "\nWrite batch cancel\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    std::vector<struct aiocb*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        struct aiocb *cb = new aiocb;
        std::string fileName = fileNames[i];

        int result = std::remove(fileName.c_str());

        int fileDescriptor = open(fileName.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }
        memset(buffer, '0'+i, bufferSize);
        // snprintf(buffer, bufferSize, "Data for file %s\n", fileName.c_str());

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_lio_opcode = LIO_WRITE;

        cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
        cb->aio_sigevent.sigev_notify_function = aio_write_callback;
        cb->aio_sigevent.sigev_notify_attributes = NULL;
        cb->aio_sigevent.sigev_value.sival_ptr = cb;

        aioList.push_back(cb);

    }

    struct aiocb* aioArray[numFiles];
    for (int i = 0; i < numFiles; ++i) {
        aioArray[i] = aioList[i];
        // std::cout << aioArray[i]->aio_fildes << "\n";
    }

    auto write_start = GetUnixTimeUs();

    // Use lio_listio to simultaneously submit multiple aio requests
    if (lio_listio(LIO_NOWAIT, aioArray, numFiles, nullptr) == -1) {
        perror("lio_listio");
        return 1;
    }

    int cacnel_num = numFiles/10;
    for (int i = numFiles-1; i >= numFiles-cacnel_num; --i) {
        if (aio_cancel(aioList[i]->aio_fildes, aioList[i]) == AIO_CANCELED) {
            printf("Request %d was successfully canceled.\n", aioList[i]->aio_fildes);
        } else if (aio_error(aioList[i]) == EINPROGRESS) {
            printf("Request %d is still in progress.\n", aioList[i]->aio_fildes);
        } else {
            perror("aio_error");
            return 1;
        }
    }

    auto submit_finished = GetUnixTimeUs();

    for (int i = 0; i < numFiles; ++i) {
        aio_suspend(aioList.data(), numFiles, nullptr);
    }

    auto write_finished = GetUnixTimeUs();

    sleep(2);

    std::cout << "Write:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << write_finished - write_start << "us\n"
              << "Submit dur: " << submit_finished - write_start << "us\n"
              << "IO cnt: " << cnt_io_
              << "\n\n";

    // Clean up
    for (int i = 0; i < numFiles; ++i) {
        close(aioList[i]->aio_fildes);
    }
    return 0;
}


enum AIOState : uint32_t {
  kInit,
  kSubmitted,
  kDone,
  kError
};

struct IOCTX {
  aiocb* cb = nullptr;
  AIOState io_stat = kInit;
};

void aio_completion_handler(int signo, siginfo_t *info, void *context) {
    struct IOCTX *aio = (struct IOCTX *)info->si_value.sival_ptr;
    auto aio_cb = aio->cb;
    if (aio_error(aio_cb) == 0) {
      aio->io_stat = kDone;
      // printf("AIO operation completed successfully.\n");
    } else {
      aio->io_stat = kError;
      printf("AIO operation failed.\n");
    }
}


int AIOBatchReadSIGNAL() {
  std::cout << "\nRead batch Signal\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    std::vector<struct IOCTX*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        aiocb *cb = new aiocb;
        IOCTX *ctx = new IOCTX;
        ctx->cb = cb;

        std::string fileName = fileNames[i];

        int fileDescriptor = open(fileName.c_str(), O_RDONLY | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }
        memset(buffer, 0, bufferSize);

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_lio_opcode = LIO_READ;

        cb->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
        cb->aio_sigevent.sigev_signo = SIGUSR1;
        cb->aio_sigevent.sigev_value.sival_ptr = ctx;

        aioList.push_back(ctx);

    }

    struct aiocb* aioArray[numFiles];
    for (int i = 0; i < numFiles; ++i) {
        aioArray[i] = aioList[i]->cb;
        // std::cout << aioArray[i]->aio_fildes << "\n";
    }

    struct sigaction sa;
    // 设置信号处理程序
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = aio_completion_handler;
    sigaction(SIGUSR1, &sa, NULL);

    auto _start = GetUnixTimeUs();

    // Use lio_listio to simultaneously submit multiple aio requests
    // LIO_WAIT可以阻塞调用这个方法
    if (lio_listio(LIO_NOWAIT, aioArray, numFiles, nullptr) == -1) {
        perror("lio_listio");
        return 1;
    }

    auto submit_finished = GetUnixTimeUs();

    int finished_cnt = 0;
    while (finished_cnt < numFiles) {
      finished_cnt = 0;
      for (int i = 0; i < numFiles; i++) {
        if(aioList[i]->io_stat == kDone || aioList[i]->io_stat == kError) {
          finished_cnt++;
        }
      }
    }

    auto _finished = GetUnixTimeUs();

    sleep(1);

    std::cout << "Read:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << _finished - _start << "us\n"
              << "Submit dur: " << submit_finished - _start << "us\n"
              << "IO cnt: " << cnt_io_
              << "\n\n";
    for (int i = 0; i < numFiles; i++)
    {
      if(!CheckArr(const_cast<void*>(aioArray[i]->aio_buf), '0'+i, bufferSize)) {
        std::cout << "Read error " << i << "\n";
        break;
      }
      free(const_cast<void*>(aioArray[i]->aio_buf));
      free(aioList[i]);
    }  

    return 0;
}


int AIOBatchReadPolling() {
  std::cout << "\nRead batch Signal\n";
  std::vector<std::string> fileNames;
    for (int i = 0; i < numFiles; ++i) {
        fileNames.push_back("aio_file" + std::to_string(i) + ".txt");
    }

    std::vector<struct IOCTX*> aioList;
    aioList.reserve(numFiles);

    for (int i = 0; i < numFiles; ++i) {
        aiocb *cb = new aiocb;
        IOCTX *ctx = new IOCTX;
        ctx->cb = cb;

        std::string fileName = fileNames[i];

        int fileDescriptor = open(fileName.c_str(), O_RDONLY | O_DIRECT);

        if (fileDescriptor == -1) {
            perror("open");
            return 1;
        }

        // Allocate a buffer for data
        char *buffer;
        if(posix_memalign((void **)&buffer, 512, bufferSize) != 0) {
          std::cout << "Alloc write buf mem failed!\n";
          abort();
        }
        memset(buffer, 0, bufferSize);
        // snprintf(buffer, bufferSize, "Data for file %s\n", fileName.c_str());

        // Initialize the aiocb structure
        memset(cb, 0, sizeof(struct aiocb));
        cb->aio_fildes = fileDescriptor;
        cb->aio_offset = 0;
        cb->aio_buf = buffer;
        cb->aio_nbytes = bufferSize;
        cb->aio_lio_opcode = LIO_READ;

        aioList.push_back(ctx);

    }

    struct aiocb* aioArray[numFiles];
    for (int i = 0; i < numFiles; ++i) {
        aioArray[i] = aioList[i]->cb;
        // std::cout << aioArray[i]->aio_fildes << "\n";
    }


    auto _start = GetUnixTimeUs();

    // Use lio_listio to simultaneously submit multiple aio requests
    // LIO_WAIT可以阻塞调用这个方法
    if (lio_listio(LIO_NOWAIT, aioArray, numFiles, nullptr) == -1) {
        perror("lio_listio");
        return 1;
    }

    auto submit_finished = GetUnixTimeUs();

    int finished_cnt = 0;
    while (finished_cnt < numFiles) {
      finished_cnt = 0;
      for (int i = 0; i < numFiles; i++) {
        int ret = aio_return(aioList[i]->cb);
        if(ret > 0) {
          finished_cnt++;
        } else if(ret == -1) {
          printf("AIO operation failed.\n");
        }
      }
    }

    auto _finished = GetUnixTimeUs();

    sleep(1);

    std::cout << "Read:" << bufferSize * numFiles << " Bytes\n" 
              << "Duration: " << _finished - _start << "us\n"
              << "Submit dur: " << submit_finished - _start << "us\n"
              << "IO cnt: " << cnt_io_
              << "\n\n";
    for (int i = 0; i < numFiles; i++)
    {
      if(!CheckArr(const_cast<void*>(aioArray[i]->aio_buf), '0'+i, bufferSize)) {
        std::cout << "Read error " << i << "\n";
        break;
      }
      free(const_cast<void*>(aioArray[i]->aio_buf));
      free(aioList[i]);
    }  

    return 0;
}


int main(int argc, char **argv) {
    for (int i = 0; i < argc; i++)
    {
      std::cout << argv[i] << "\n";
    }
    int workload = 6;
    if(argc == 2) {
      numFiles = std::stoi(argv[1]);
    } else if(argc == 3) {
      workload = std::stoi(argv[2]);
    }
    // bufferSize = 4 << 10;
    // AIOBatchWrite();
    if (workload == 0) {
      AIOBatchWrite();
    } else if (workload == 1) {
      AIONoBatchWrite();
    } else if (workload == 2) {
      AIONoBatchRead();
    } else if (workload == 3) {
      AIOBatchRead();
    } else if (workload == 4) {
      TestWriteCancel();
    } else if (workload == 5) {
      TestWriteBatchCancel();
    } else if (workload == 6) {
      AIOBatchReadSIGNAL();
    }

    return 0;
}
