#pragma once

// 单线程协程调度器（header-only）。
//
// 调度模型：
//   * 所有协程运行在同一个线程上，采用协作式调度。协程只有在 co_await 处
//     才会让出 CPU，因此 CPU 密集型任务必须周期性 co_await Yield()，
//     否则会阻塞整个事件循环（定时器不会触发、IO 事件不会被处理）。
//   * 三类挂起源：就绪队列（Yield）、定时器（SleepFor/SleepUntil）、
//     epoll（Readable/Writable/WaitFd）。
//   * 任务的生命周期由调度器接管：Add() 之后协程句柄的所有权转移给
//     BatchTask，在 Run() 结束时或 BatchTask 析构时统一销毁。
//
// 使用方式：
//   Task Reader(BatchTask& bt, int fd) { ... }   // 普通协程函数
//
//   BatchTask bt;
//   bt.Add(Reader(bt, fd), "reader");
//   bt.AddSync([] { /* 同步小任务 */ }, "sync");
//   bt.Run();
//
// 生命周期契约（重要）：
//   Add() 只接受已经创建好的 Task。请务必用「普通协程函数」或「具名 lambda」
//   来创建它，不要把一个临时 lambda 交给调度器去调用。
//
//   原因是 lambda 协程的闭包对象属于隐式对象参数，标准并不要求它被拷贝进
//   协程帧；它的存活由调用方负责。若由 Add() 内部持有一份局部副本再去调用，
//   Add() 返回后该副本即销毁，协程恢复时就读到了悬垂内存。这类代码在 -O2
//   下常常"碰巧"能跑，在 -O0 下则读到垃圾值，是典型的未定义行为。
//
//   同理，lambda 按引用捕获的变量必须在协程结束前一直有效，这一点与
//   std::thread 的要求一致。
//
// 线程安全：非线程安全，只能在创建它的线程上使用。

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <queue>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace kaixyhe::batch_task {

// 单次 epoll_wait 最多取回的事件数。
constexpr int kMaxEpollEvents = 64;

// 传给 epoll_wait 的"无限等待"超时值。没有定时器时用它，纯等 IO 事件。
constexpr int kNoTimeoutMs = -1;

// Readable()/Writable() 默认关注的事件掩码。错误与挂断总是被包含，
// 这样对端关闭连接时协程能被唤醒，而不是永远挂起。
constexpr std::uint32_t kReadEvents = EPOLLIN | EPOLLERR | EPOLLHUP;
constexpr std::uint32_t kWriteEvents = EPOLLOUT | EPOLLERR | EPOLLHUP;

// 下面是 co_await Readable(fd) / Writable(fd) 返回事件掩码的判读辅助。

// 是否可读（对端有数据，或对端已关闭，此时 read() 返回 0）。
constexpr bool IsReadable(std::uint32_t events) noexcept {
  return (events & EPOLLIN) != 0;
}

// 是否可写。
constexpr bool IsWritable(std::uint32_t events) noexcept {
  return (events & EPOLLOUT) != 0;
}

// 是否发生错误或对端挂断。唤醒后应先检查它再决定是否继续读写。
constexpr bool HasError(std::uint32_t events) noexcept {
  return (events & (EPOLLERR | EPOLLHUP)) != 0;
}

class BatchTask;

namespace internal {

// epoll 句柄的 RAII 封装，保证异常路径下不会泄漏文件描述符。
class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  ~UniqueFd() { Reset(); }

  int get() const noexcept { return fd_; }
  bool valid() const noexcept { return fd_ >= 0; }

  void Reset() noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_ = -1;
};

}  // namespace internal

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

// 一个可被 BatchTask 调度的协程。
//
// Task 不可拷贝、可移动；移动后源对象不再持有协程句柄。
// Add() 之后句柄所有权归 BatchTask，不要再手动移动或销毁它。
class Task {
 public:
  struct Promise;
  // 协程机制要求返回类型里必须有一个名为 promise_type 的成员类型。
  using promise_type = Promise;
  using Handle = std::coroutine_handle<Promise>;

  Task() = default;
  explicit Task(Handle handle) noexcept : handle_(handle) {}

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      DestroyIfOwned();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }

  ~Task() { DestroyIfOwned(); }

  // 是否持有协程句柄。默认构造或已被移动走的 Task 返回 false。
  bool valid() const noexcept { return handle_ != nullptr; }

 private:
  friend class BatchTask;

  // 交出句柄所有权，调用后本对象不再持有句柄。
  Handle Release() noexcept { return std::exchange(handle_, {}); }

  void DestroyIfOwned() noexcept {
    if (handle_ != nullptr) {
      handle_.destroy();
      handle_ = nullptr;
    }
  }

  Handle handle_ = nullptr;
};

// Task 的协程承诺对象。调度器通过 owner 反向引用到自己。
struct Task::Promise {
  // 调度器在 Add() 时填写，用于协程结束时回调 Finish()。
  BatchTask* owner = nullptr;
  // 任务名，仅用于异常定位与日志，可为空。
  std::string name;
  // 协程体内未捕获的异常，由 unhandled_exception 保存，延迟到 Run() 抛出。
  std::exception_ptr exception;

  Task get_return_object() { return Task{Handle::from_promise(*this)}; }

  // 惰性启动：创建协程后先挂起，等 Run() 把它放进就绪队列再执行。
  std::suspend_always initial_suspend() noexcept { return {}; }

  // 协程结束后不自行销毁，而是把控制权交回调度器，
  // 由调度器统一回收并递减活跃任务计数。
  struct FinalAwaiter {
    bool await_ready() noexcept { return false; }
    void await_resume() noexcept {}
    void await_suspend(Handle handle) noexcept;
  };

  FinalAwaiter final_suspend() noexcept { return {}; }
  void return_void() noexcept {}

  // 吞下异常而不是立即抛出：协程此刻可能正被调度器 resume，
  // 直接抛出会穿过调度器栈帧，导致其余任务无法继续调度。
  void unhandled_exception() noexcept { exception = std::current_exception(); }
};

// ---------------------------------------------------------------------------
// BatchTask
// ---------------------------------------------------------------------------

// 驱动一组协程直到全部结束的调度器。
//
//   Task Reader(BatchTask& bt, int fd) {
//     co_await bt.SleepFor(50ms);
//   }
//
//   BatchTask bt;
//   bt.Add(Reader(bt, fd), "reader");
//   bt.Run();
//   std::cout << bt.stats().completed_tasks;
class BatchTask {
 public:
  using Clock = std::chrono::steady_clock;
  using Handle = Task::Handle;

  // 调度器运行统计。Run() 可以多次调用，统计会跨调用累加。
  struct Stats {
    std::size_t scheduled_tasks = 0;   // Add() 成功接纳的任务数
    std::size_t completed_tasks = 0;   // 正常跑到 co_return 的任务数
    std::size_t failed_tasks = 0;      // 抛出未捕获异常的任务数
    std::size_t cancelled_tasks = 0;   // 被 Stop() 或析构中断的任务数
    std::size_t resume_count = 0;      // 协程被恢复的总次数
    std::size_t epoll_wait_count = 0;  // 进入内核等待的次数
    Clock::duration elapsed{};         // Run() 累计耗时
  };

  BatchTask() : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)) {
    if (!epoll_fd_.valid()) {
      throw std::system_error(errno, std::generic_category(),
                              "epoll_create1 failed");
    }
  }

  BatchTask(const BatchTask&) = delete;
  BatchTask& operator=(const BatchTask&) = delete;

  // 析构会销毁所有尚未结束的协程，并执行它们栈上对象的析构函数。
  ~BatchTask() { DestroyAllPending(); }

  // 把一个已经创建好的协程任务交给调度器。
  // name 仅用于异常定位，可为空。任务此时并未开始执行，要等 Run()。
  //
  // 请传入「普通协程函数的调用结果」或「具名 lambda 的调用结果」：
  //     bt.Add(Reader(bt, fd), "reader");
  //     auto reader = [&](BatchTask& bt) -> Task { ... };
  //     bt.Add(reader(bt), "reader");
  // 不要期望调度器帮你保管 lambda 的闭包对象，详见文件头注释。
  void Add(Task task, std::string name = {}) {
    Handle handle = task.Release();
    if (handle == nullptr) {
      return;
    }
    handle.promise().owner = this;
    handle.promise().name = std::move(name);
    ++active_tasks_;
    ++stats_.scheduled_tasks;
    Schedule(handle);
  }

  // 添加一个同步小任务，签名可以是 void() 或 void(BatchTask&)。
  //
  // 与 Add() 不同，这里可以安全地传入临时 lambda：可调用对象会作为协程的
  // 值参数被拷贝进协程帧，不依赖调用方的栈。
  template <typename Fn>
  void AddSync(Fn fn, std::string name = {}) {
    Add(MakeSyncTask(std::move(fn), *this), std::move(name));
  }

  // 驱动所有已添加的任务直到全部结束。可以多次调用。
  //
  // 若有任务抛出未捕获异常，Run() 会等所有任务结束后再重新抛出第一个异常，
  // 以免提前退出导致其他任务的资源得不到清理；失败总数见 stats()，
  // 首个失败任务名见 failed_task_name()。
  void Run() {
    const Clock::time_point start = Clock::now();

    while (active_tasks_ > 0 && !stopping_) {
      ResumeReady();
      DestroyFinished();
      MoveExpiredTimersToReady();

      if (!ready_.empty()) {
        continue;
      }
      if (active_tasks_ == 0) {
        break;
      }
      if (fd_waiters_.empty() && timers_.empty()) {
        // 既没有可跑的任务，也没有定时器和 IO 等待者，再等下去也不会有进展。
        throw std::runtime_error(BuildDeadlockMessage());
      }

      WaitEvents();
    }

    if (stopping_) {
      // 停止请求：回收所有挂起的任务，让调度器回到可以重新使用的空状态。
      DestroyAllPending();
      stopping_ = false;
    }
    DestroyFinished();
    stats_.elapsed += Clock::now() - start;

    if (first_exception_ != nullptr) {
      std::rethrow_exception(first_exception_);
    }
  }

  // 请求停止。不会粗暴中断正在运行的协程，而是在本次 Run() 返回前统一清理
  // 挂起的任务。Run() 返回后可以重新 Add() 并再次 Run()。
  void Stop() noexcept { stopping_ = true; }

  // 尚未结束的任务数。
  std::size_t pending_tasks() const noexcept { return active_tasks_; }

  // 运行统计，跨 Run() 调用累加。
  const Stats& stats() const noexcept { return stats_; }

  // 首个抛出未捕获异常的任务名，没有失败任务时为空。
  const std::string& failed_task_name() const noexcept {
    return failed_task_name_;
  }

  // -------------------------------------------------------------------------
  // Awaiter
  // -------------------------------------------------------------------------

  // 让出 CPU，把当前协程放回就绪队列尾部。
  // CPU 密集型循环应周期性 co_await Yield()，否则会饿死其他任务。
  struct YieldAwaiter {
    BatchTask& owner;
    bool await_ready() const noexcept { return false; }
    void await_resume() const noexcept {}
    void await_suspend(Handle handle) const { owner.Schedule(handle); }
  };

  YieldAwaiter Yield() noexcept { return YieldAwaiter{*this}; }

  // 挂起一段时间。若时长已过（例如传 0）则直接继续，不进入定时器队列。
  struct SleepAwaiter {
    BatchTask& owner;
    Clock::time_point expire_at;

    bool await_ready() const noexcept { return Clock::now() >= expire_at; }
    void await_resume() const noexcept {}
    void await_suspend(Handle handle) const {
      owner.AddTimer(expire_at, handle);
    }
  };

  template <typename Rep, typename Period>
  SleepAwaiter SleepFor(std::chrono::duration<Rep, Period> duration) {
    return SleepAwaiter{*this, Clock::now() +
           std::chrono::duration_cast<Clock::duration>(duration)};
  }

  SleepAwaiter SleepUntil(Clock::time_point expire_at) noexcept {
    return SleepAwaiter{*this, expire_at};
  }

  // 等待 fd 上发生指定 epoll 事件；co_await 的结果是实际发生的事件掩码，
  // 可用 IsReadable()/IsWritable()/HasError() 判读。
  //
  // 只唤醒真正等到了自己所关注事件的等待者：若同一 fd 上有人等 EPOLLIN、
  // 有人等 EPOLLOUT，那么 EPOLLIN 到达时只有前者会被唤醒。
  struct FdAwaiter {
    BatchTask& owner;
    int fd = -1;
    std::uint32_t events = 0;
    std::uint32_t occurred = 0;

    bool await_ready() const noexcept { return false; }
    std::uint32_t await_resume() const noexcept { return occurred; }
    void await_suspend(Handle handle) {
      owner.RegisterFdWaiter(fd, events, handle, this);
    }
  };

  FdAwaiter Readable(int fd) noexcept {
    return FdAwaiter{*this, fd, kReadEvents};
  }

  FdAwaiter Writable(int fd) noexcept {
    return FdAwaiter{*this, fd, kWriteEvents};
  }

  FdAwaiter WaitFd(int fd, std::uint32_t events) noexcept {
    return FdAwaiter{*this, fd, events};
  }

 private:
  friend struct Task::Promise::FinalAwaiter;

  // 定时器项。seq 用于在同一到期时刻时保证先进先出，避免相同时间戳下
  // std::priority_queue 的顺序不确定。
  struct TimerItem {
    Clock::time_point expire_at;
    std::uint64_t seq = 0;
    Handle handle = nullptr;

    bool operator>(const TimerItem& other) const noexcept {
      if (expire_at != other.expire_at) {
        return expire_at > other.expire_at;
      }
      return seq > other.seq;
    }
  };

  // 一个挂在 fd 上的等待者。
  struct FdWaiter {
    std::uint32_t events = 0;
    Handle handle = nullptr;
    // 指向协程栈上的 awaiter。事件到达时把结果写回它；
    // 协程被销毁时整个等待者会一并从表中移除，不会出现悬垂访问。
    FdAwaiter* awaiter = nullptr;
  };

  // -------------------------------------------------------------------------
  // 调度内部
  // -------------------------------------------------------------------------

  // 把一个同步可调用对象包成协程任务。
  //
  // 有意写成「普通函数」而不是 lambda：协程的值参数会被拷贝进协程帧，
  // 因此 fn 不依赖调用方栈帧，AddSync() 传入临时 lambda 也是安全的。
  template <typename Fn>
  static Task MakeSyncTask(Fn fn, BatchTask& bt) {
    if constexpr (std::is_invocable_v<Fn&, BatchTask&>) {
      fn(bt);
    } else if constexpr (std::is_invocable_v<Fn&>) {
      fn();
    } else {
      static_assert(std::is_invocable_v<Fn&> ||
                        std::is_invocable_v<Fn&, BatchTask&>,
                    "BatchTask::AddSync expects a callable with signature "
                    "void() or void(BatchTask&)");
    }
    co_return;
  }

  void Schedule(Handle handle) { ready_.push_back(handle); }

  // 协程运行结束（co_return 或抛出未捕获异常）后的收尾。
  void Finish(Handle handle) noexcept {
    Task::Promise& promise = handle.promise();
    if (promise.exception != nullptr) {
      ++stats_.failed_tasks;
      if (first_exception_ == nullptr) {
        first_exception_ = promise.exception;
        failed_task_name_ = promise.name;
      }
    } else {
      ++stats_.completed_tasks;
    }
    destroy_later_.push_back(handle);
    if (active_tasks_ > 0) {
      --active_tasks_;
    }
  }

  // 依次恢复就绪队列中的协程。
  //
  // 每恢复一个就回收已结束的协程并把到期定时器移入就绪队列，这样在纯计算
  // 场景下不需要绕道 epoll_wait 就能继续推进。
  void ResumeReady() {
    while (!ready_.empty()) {
      Handle handle = ready_.front();
      ready_.pop_front();
      if (handle != nullptr && !handle.done()) {
        ++stats_.resume_count;
        handle.resume();
      }
      DestroyFinished();
      MoveExpiredTimersToReady();
    }
  }

  // 销毁上一轮结束的协程。延迟销毁是必要的：协程在 final_suspend 时仍处于
  // 自己的栈帧上，立即 destroy 会破坏正在展开的调用栈。
  void DestroyFinished() noexcept {
    for (Handle handle : destroy_later_) {
      if (handle != nullptr) {
        handle.destroy();
      }
    }
    destroy_later_.clear();
  }

  void AddTimer(Clock::time_point expire_at, Handle handle) {
    timers_.push(TimerItem{expire_at, ++timer_seq_, handle});
  }

  void MoveExpiredTimersToReady() {
    const Clock::time_point now = Clock::now();
    while (!timers_.empty() && timers_.top().expire_at <= now) {
      Handle handle = timers_.top().handle;
      timers_.pop();
      Schedule(handle);
    }
  }

  // 计算下一次 epoll_wait 的超时时间：取最近的定时器，没有则无限等待。
  int NextTimeoutMs() const noexcept {
    if (stopping_) {
      return 0;  // 已请求停止，尽快脱离内核等待
    }
    if (timers_.empty()) {
      return kNoTimeoutMs;
    }
    const Clock::time_point now = Clock::now();
    if (timers_.top().expire_at <= now) {
      return 0;
    }
    const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
        timers_.top().expire_at - now);
    return static_cast<int>(diff.count() > 0 ? diff.count() : 1);
  }

  // 把一个等待者登记到 fd 上，必要时调整 epoll 的关注集合。
  void RegisterFdWaiter(int fd, std::uint32_t events, Handle handle,
                        FdAwaiter* awaiter) {
    if (fd < 0) {
      throw std::invalid_argument("BatchTask: fd must be non-negative");
    }

    std::vector<FdWaiter>& waiters = fd_waiters_[fd];
    const bool first_waiter = waiters.empty();
    waiters.push_back(FdWaiter{events, handle, awaiter});

    epoll_event ev{};
    ev.events = CollectEvents(waiters);
    ev.data.fd = fd;
    const int op = first_waiter ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    if (::epoll_ctl(epoll_fd_.get(), op, fd, &ev) != 0) {
      waiters.pop_back();
      if (waiters.empty()) {
        fd_waiters_.erase(fd);
      }
      throw std::system_error(errno, std::generic_category(),
                              "epoll_ctl failed");
    }
  }

  static std::uint32_t CollectEvents(
      const std::vector<FdWaiter>& waiters) noexcept {
    std::uint32_t events = 0;
    for (const FdWaiter& waiter : waiters) {
      events |= waiter.events;
    }
    return events;
  }

  void WaitEvents() {
    epoll_event events[kMaxEpollEvents];
    ++stats_.epoll_wait_count;
    const int n = ::epoll_wait(epoll_fd_.get(), events, kMaxEpollEvents,
                               NextTimeoutMs());
    if (n < 0) {
      if (errno == EINTR) {
        return;
      }
      throw std::system_error(errno, std::generic_category(),
                              "epoll_wait failed");
    }

    for (int i = 0; i < n; ++i) {
      const int fd = events[i].data.fd;
      const std::uint32_t occurred = events[i].events;
      const auto it = fd_waiters_.find(fd);
      if (it == fd_waiters_.end()) {
        continue;
      }

      // 只唤醒等到了自己关注事件的等待者；其余的继续挂在同一个 fd 上。
      // 早期版本会把该 fd 上的所有等待者一并唤醒，导致等待 EPOLLOUT 的
      // 协程被一次 EPOLLIN  spurious 唤醒，随后在尚未可写时写入失败。
      std::vector<FdWaiter> remaining;
      remaining.reserve(it->second.size());
      for (FdWaiter& waiter : it->second) {
        if ((waiter.events & occurred) == 0) {
          remaining.push_back(waiter);
          continue;
        }
        if (waiter.awaiter != nullptr) {
          waiter.awaiter->occurred = occurred;
        }
        Schedule(waiter.handle);
      }

      if (remaining.empty()) {
        fd_waiters_.erase(it);
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr);
      } else {
        it->second = std::move(remaining);
        epoll_event ev{};
        ev.events = CollectEvents(it->second);
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, fd, &ev) != 0) {
          throw std::system_error(errno, std::generic_category(),
                                  "epoll_ctl(MOD) failed");
        }
      }
    }
  }

  std::string BuildDeadlockMessage() const {
    return "BatchTask deadlock: " + std::to_string(active_tasks_) +
           " task(s) still pending, but there is no ready task, no timer and "
           "no fd waiter. A CPU-bound task probably forgot to co_await "
           "Yield().";
  }

  // 销毁所有尚未结束的协程，并与 epoll 解绑，让调度器回到空状态。
  // 用于析构与 Stop() 之后的清理；任务既不计入完成也不计入失败。
  void DestroyAllPending() noexcept {
    DestroyFinished();
    for (Handle handle : ready_) {
      if (handle != nullptr) {
        ++stats_.cancelled_tasks;
        handle.destroy();
      }
    }
    ready_.clear();

    while (!timers_.empty()) {
      Handle handle = timers_.top().handle;
      timers_.pop();
      if (handle != nullptr) {
        ++stats_.cancelled_tasks;
        handle.destroy();
      }
    }

    for (auto& [fd, waiters] : fd_waiters_) {
      for (FdWaiter& waiter : waiters) {
        if (waiter.handle != nullptr) {
          ++stats_.cancelled_tasks;
          waiter.handle.destroy();
        }
      }
      if (epoll_fd_.valid()) {
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr);
      }
    }
    fd_waiters_.clear();

    active_tasks_ = 0;
  }

  internal::UniqueFd epoll_fd_;
  bool stopping_ = false;
  std::size_t active_tasks_ = 0;
  std::uint64_t timer_seq_ = 0;
  Stats stats_;
  std::exception_ptr first_exception_;
  std::string failed_task_name_;
  std::deque<Handle> ready_;
  std::vector<Handle> destroy_later_;
  std::priority_queue<TimerItem, std::vector<TimerItem>,
                      std::greater<TimerItem>>
      timers_;
  std::unordered_map<int, std::vector<FdWaiter>> fd_waiters_;
};

inline void Task::Promise::FinalAwaiter::await_suspend(Handle handle) noexcept {
  if (handle.promise().owner != nullptr) {
    handle.promise().owner->Finish(handle);
  }
}

}  // namespace kaixyhe::batch_task
