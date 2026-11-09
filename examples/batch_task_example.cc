#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "kaixyhe/batch_task/batch_task.h"

using kaixyhe::batch_task::BatchTask;
using kaixyhe::batch_task::HasError;
using kaixyhe::batch_task::IsReadable;
using kaixyhe::batch_task::Task;
using namespace std::chrono_literals;

namespace {

// 1) 定时器任务。写成普通协程函数：参数按值进入协程帧，不存在闭包悬垂问题。
Task Writer(BatchTask& bt, int fd) {
  co_await bt.SleepFor(50ms);
  const char* msg = "hello from io task";
  ::write(fd, msg, std::strlen(msg));
}

// 2) IO 任务。co_await 的结果是本轮实际发生的事件掩码。
//    先读数据再处理错误标志：对端关闭时 EPOLLIN 与 EPOLLHUP 会同时置位，
//    缓冲区里可能还有未读走的数据。
Task Reader(BatchTask& bt, int fd) {
  const std::uint32_t events = co_await bt.Readable(fd);
  if (IsReadable(events)) {
    char buf[128] = {};
    const ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
    if (n > 0) {
      std::cout << "reader got: " << buf << "\n";
    }
  }
  if (HasError(events)) {
    std::cout << "reader: pipe error or peer closed\n";
  }
}

// 3) CPU 密集型任务：必须周期性 co_await Yield()。
//    调度是协作式的，不主动让出就会阻塞整个事件循环，
//    writer 的定时器和 reader 的 IO 都得不到推进。
Task Compute(BatchTask& bt) {
  long long sum = 0;
  for (int i = 0; i < 1'000'000; ++i) {
    sum += i;
    if (i % 100'000 == 0) {
      co_await bt.Yield();
    }
  }
  std::cout << "compute sum: " << sum << "\n";
}

void PrintStats(const BatchTask& bt) {
  const BatchTask::Stats& s = bt.stats();
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(s.elapsed).count();
  std::cout << "\n--- stats ---\n"
            << "scheduled: " << s.scheduled_tasks << '\n'
            << "completed: " << s.completed_tasks << '\n'
            << "failed:    " << s.failed_tasks << '\n'
            << "cancelled: " << s.cancelled_tasks << '\n'
            << "resumes:   " << s.resume_count << '\n'
            << "epoll:     " << s.epoll_wait_count << '\n'
            << "elapsed:   " << elapsed_ms << " ms\n";
}

}  // namespace

int main() {
  BatchTask bt;

  int pipe_fd[2];
  if (::pipe(pipe_fd) != 0) {
    std::cerr << "pipe failed\n";
    return 1;
  }
  ::fcntl(pipe_fd[0], F_SETFL, ::fcntl(pipe_fd[0], F_GETFL, 0) | O_NONBLOCK);
  ::fcntl(pipe_fd[1], F_SETFL, ::fcntl(pipe_fd[1], F_GETFL, 0) | O_NONBLOCK);

  // 第二个参数是任务名，只用于异常定位，可省略。
  bt.Add(Writer(bt, pipe_fd[1]), "writer");
  bt.Add(Reader(bt, pipe_fd[0]), "reader");
  bt.Add(Compute(bt), "compute");

  // 同步小任务：可调用对象会被拷进协程帧，传临时 lambda 是安全的。
  bt.AddSync([] { std::cout << "sync task done\n"; }, "sync");

  bt.Run();

  PrintStats(bt);

  ::close(pipe_fd[0]);
  ::close(pipe_fd[1]);
  return 0;
}
