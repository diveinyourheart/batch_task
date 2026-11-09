// batch_task 自测。不依赖 gtest，直接返回非 0 表示失败。
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

#include "kaixyhe/batch_task/batch_task.h"

using kaixyhe::batch_task::BatchTask;
using kaixyhe::batch_task::IsReadable;
using kaixyhe::batch_task::IsWritable;
using kaixyhe::batch_task::Task;
using namespace std::chrono_literals;

namespace {

#define CHECK(cond)                                                       \
  do {                                                                    \
    if (!(cond)) {                                                        \
      std::cerr << "FAILED " << __func__ << ":" << __LINE__ << ": "       \
                << #cond << "\n";                                         \
      return 1;                                                           \
    }                                                                     \
  } while (false)

// 不注册任何等待源的 awaiter，用来构造死锁场景。
struct NoOpAwaiter {
  bool await_ready() const noexcept { return false; }
  void await_resume() const noexcept {}
  void await_suspend(std::coroutine_handle<>) noexcept {}
};

// ---------------------------------------------------------------------------
// 同一 fd 上等待不同事件的两个任务，只有等到自己所关注事件的那个会被唤醒。
// ---------------------------------------------------------------------------
Task WaitRead(BatchTask& bt, int fd, std::uint32_t* events_out,
              std::string* data_out) {
  *events_out = co_await bt.Readable(fd);
  char buf[64] = {};
  const ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
  if (n > 0) {
    *data_out = buf;
  }
}

Task WaitWrite(BatchTask& bt, int fd, std::uint32_t* events_out) {
  *events_out = co_await bt.Writable(fd);
}

Task DelayedWrite(BatchTask& bt, int fd) {
  co_await bt.SleepFor(20ms);
  ::write(fd, "ping", 4);
}

int TestFdEventsAreMatchedPerWaiter() {
  int sv[2];
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
    std::cerr << "socketpair failed\n";
    return 1;
  }

  std::uint32_t read_events = 0;
  std::uint32_t write_events = 0;
  std::string data;

  BatchTask bt;
  bt.Add(WaitRead(bt, sv[0], &read_events, &data), "wait-read");
  bt.Add(WaitWrite(bt, sv[0], &write_events), "wait-write");
  bt.Add(DelayedWrite(bt, sv[1]), "delayed-write");
  bt.Run();

  // 等 EPOLLOUT 的任务先被唤醒，且不应被 EPOLLIN 误唤醒。
  CHECK(IsWritable(write_events));
  CHECK(!IsReadable(write_events));

  // 等 EPOLLIN 的任务只在真正可读后才被唤醒，并能读到数据。
  CHECK(IsReadable(read_events));
  CHECK(data == "ping");

  CHECK(bt.stats().completed_tasks == 3);
  CHECK(bt.stats().failed_tasks == 0);

  ::close(sv[0]);
  ::close(sv[1]);
  return 0;
}

// ---------------------------------------------------------------------------
// 具名 lambda：引用捕获在调用方作用域内保持有效。
// ---------------------------------------------------------------------------
int TestNamedLambdaWithReferenceCapture() {
  int captured = 7;
  int result = 0;

  auto task = [&](BatchTask& bt) -> Task {
    co_await bt.Yield();
    result = captured * 2;
  };

  BatchTask bt;
  bt.Add(task(bt), "named-lambda");
  bt.Run();

  CHECK(result == 14);
  CHECK(bt.stats().completed_tasks == 1);
  return 0;
}

// ---------------------------------------------------------------------------
// AddSync 接受临时 lambda，可调用对象被拷进协程帧。
// ---------------------------------------------------------------------------
int TestAddSyncAcceptsTemporaryLambda() {
  int calls = 0;
  BatchTask bt;
  bt.AddSync([&calls] { ++calls; }, "sync-no-arg");
  bt.AddSync([&calls](BatchTask&) { ++calls; }, "sync-with-arg");
  bt.Run();

  CHECK(calls == 2);
  CHECK(bt.stats().completed_tasks == 2);
  return 0;
}

// ---------------------------------------------------------------------------
// 任务抛出的异常在 Run() 末尾重新抛出，其余任务不受影响。
// ---------------------------------------------------------------------------
Task Throwing(BatchTask& bt) {
  co_await bt.Yield();
  throw std::runtime_error("boom");
}

Task Counting(BatchTask& bt, int* counter) {
  co_await bt.Yield();
  ++*counter;
}

int TestExceptionPropagation() {
  int counter = 0;
  BatchTask bt;
  bt.Add(Throwing(bt), "throwing");
  bt.Add(Counting(bt, &counter), "counting");

  bool caught = false;
  try {
    bt.Run();
  } catch (const std::runtime_error& e) {
    caught = std::string(e.what()) == "boom";
  }

  CHECK(caught);
  CHECK(counter == 1);  // 另一个任务仍然跑完了
  CHECK(bt.stats().failed_tasks == 1);
  CHECK(bt.stats().completed_tasks == 1);
  CHECK(bt.failed_task_name() == "throwing");
  return 0;
}

// ---------------------------------------------------------------------------
// Stop() 后挂起的任务被回收，Run() 正常返回，调度器可继续使用。
// ---------------------------------------------------------------------------
Task HangOnRead(BatchTask& bt, int fd) {
  co_await bt.Readable(fd);
}

Task StopAfterDelay(BatchTask& bt) {
  co_await bt.SleepFor(10ms);
  bt.Stop();
}

int TestStopCancelsPendingTasks() {
  int sv[2];
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
    return 1;
  }

  BatchTask bt;
  bt.Add(HangOnRead(bt, sv[0]), "hang");
  bt.Add(StopAfterDelay(bt), "stopper");
  bt.Run();  // 不应挂死，也不应抛异常

  CHECK(bt.stats().cancelled_tasks == 1);
  CHECK(bt.stats().completed_tasks == 1);
  CHECK(bt.pending_tasks() == 0);

  // 调度器回到空状态，可以再次使用。
  int counter = 0;
  bt.Add(Counting(bt, &counter), "again");
  bt.Run();
  CHECK(counter == 1);

  ::close(sv[0]);
  ::close(sv[1]);
  return 0;
}

// ---------------------------------------------------------------------------
// 任务挂起但没注册任何等待源时，应报告死锁而不是无限阻塞。
// ---------------------------------------------------------------------------
Task Deadlocking() {
  co_await NoOpAwaiter{};
}

int TestDeadlockIsDetected() {
  BatchTask bt;
  bt.Add(Deadlocking(), "deadlock");

  bool caught = false;
  try {
    bt.Run();
  } catch (const std::runtime_error& e) {
    caught = std::string(e.what()).find("deadlock") != std::string::npos;
  }

  CHECK(caught);
  return 0;
}

// ---------------------------------------------------------------------------
// 定时器到期顺序：先到期的先恢复。
// ---------------------------------------------------------------------------
Task RecordOrder(BatchTask& bt, int delay_ms, std::string* order, char tag) {
  co_await bt.SleepFor(std::chrono::milliseconds(delay_ms));
  order->push_back(tag);
}

int TestTimerOrder() {
  std::string order;
  BatchTask bt;
  bt.Add(RecordOrder(bt, 30, &order, 'c'), "slow");
  bt.Add(RecordOrder(bt, 5, &order, 'a'), "fast");
  bt.Add(RecordOrder(bt, 15, &order, 'b'), "mid");
  bt.Run();

  CHECK(order == "abc");
  return 0;
}

}  // namespace

int main() {
  int failures = 0;
  failures += TestFdEventsAreMatchedPerWaiter();
  failures += TestNamedLambdaWithReferenceCapture();
  failures += TestAddSyncAcceptsTemporaryLambda();
  failures += TestExceptionPropagation();
  failures += TestStopCancelsPendingTasks();
  failures += TestDeadlockIsDetected();
  failures += TestTimerOrder();

  if (failures == 0) {
    std::cout << "batch_task tests passed\n";
    return 0;
  }
  std::cerr << failures << " test(s) failed\n";
  return 1;
}
