# batch_task

一个基于 Linux `epoll` 的单线程 C++20 协程调度器。

## 构建与测试

```bash
cmake --preset debug
cmake --build --preset debug
ctest --test-dir build/debug --output-on-failure
```

也可以直接配置：

```bash
cmake -S . -B build/debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/debug --parallel
ctest --test-dir build/debug --output-on-failure
```

库目标为 `kaixyhe::batch_task`。当前实现依赖 Linux 的 `epoll`，不支持其他平台。

这个 `batch_task` 写得其实很漂亮，它本质上就是一个**单线程协程调度器（Coroutine Scheduler）**。真正的核心不是 `co_await`，而是 **`Run()` 如何不断调度各个 coroutine handle**。

我按你的 example，把整个运行过程讲一遍。

## 快速开始

```cpp
#include "kaixyhe/batch_task/batch_task.h"

using kaixyhe::batch_task::BatchTask;
using kaixyhe::batch_task::Task;

Task Reader(BatchTask& bt, int fd) {
  const std::uint32_t events = co_await bt.Readable(fd);
  if (kaixyhe::batch_task::IsReadable(events)) {
    char buf[128] = {};
    ::read(fd, buf, sizeof(buf) - 1);
  }
}

Task Heartbeat(BatchTask& bt) {
  for (;;) {
    co_await bt.SleepFor(std::chrono::seconds(1));
  }
}

int main() {
  BatchTask bt;
  bt.Add(Reader(bt, fd), "reader");            // 协程任务
  bt.Add(Heartbeat(bt), "heartbeat");
  bt.AddSync([] { /* 同步小任务 */ }, "sync");  // 同步任务
  bt.Run();
}
```

## 生命周期契约（必读）

`Add()` 只接受**已经创建好的 `Task`**。请用「普通协程函数」或「具名 lambda」来创建它：

```cpp
// 正确：普通协程函数
bt.Add(Reader(bt, fd), "reader");

// 正确：具名 lambda，闭包在调用方作用域内存活
auto reader = [&](BatchTask& bt) -> Task { /* ... */ };
bt.Add(reader(bt), "reader");

// 错误：编译不过。不要把临时 lambda 交给调度器去调用
bt.Add([](BatchTask& bt) -> Task { /* ... */ }, "bad");
```

原因是 lambda 协程的闭包对象属于**隐式对象参数**，标准并不要求它被拷贝进协程帧，
它的存活由调用方负责。如果由 `Add()` 内部持有一份局部副本再去调用，`Add()`
返回后该副本即销毁，协程恢复时就读到了悬垂内存。这类代码在 `-O2` 下常常"碰巧"
能跑，在 `-O0` 下则读到垃圾值，是典型的未定义行为。

> 旧版本提供了 `Add(可调用对象)` 重载，正是踩了这个坑，现已移除。
> 同步任务请改用 `AddSync()`：可调用对象会作为协程的**值参数**被拷进协程帧，
> 因此传临时 lambda 是安全的。

同样地，lambda 按引用捕获的变量必须在协程结束前一直有效，这一点与
`std::thread` 的要求一致。

## API

| 接口 | 说明 |
|---|---|
| `Add(Task, name = {})` | 添加协程任务，任务在 `Run()` 时才开始执行 |
| `AddSync(Fn, name = {})` | 添加同步任务，签名 `void()` 或 `void(BatchTask&)` |
| `Run()` | 驱动所有任务直到结束；有任务抛异常则在最后重新抛出第一个 |
| `Stop()` | 请求停止；`Run()` 返回前回收挂起的任务，之后可继续使用 |
| `pending_tasks()` | 尚未结束的任务数 |
| `stats()` | 运行统计：调度/完成/失败/取消/恢复次数/epoll 次数/耗时 |
| `failed_task_name()` | 首个失败任务名，用于定位 |
| `co_await Yield()` | 让出 CPU，重回就绪队列尾部 |
| `co_await SleepFor(d)` / `SleepUntil(t)` | 定时器挂起 |
| `co_await Readable(fd)` / `Writable(fd)` / `WaitFd(fd, ev)` | epoll 挂起，返回实际事件掩码 |

事件判读辅助：`IsReadable()` / `IsWritable()` / `HasError()`。

## 已修复的问题

- **同一 fd 上的等待者会互相误唤醒**：早期版本在事件到达时把该 fd 上的所有等待者
  一并唤醒。现在只唤醒 `waiter.events & occurred != 0` 的等待者，其余继续挂起，
  等待 EPOLLOUT 的协程不会再被一次 EPOLLIN spurious 唤醒。
- **`Add(可调用对象)` 的悬垂闭包**：见上一节，该重载已移除。
- **epoll fd 未做 RAII**：异常路径下会泄漏，现由 `internal::UniqueFd` 管理。
- **`Stop()` 语义不清**：旧实现直接把活跃计数清零，`Run()` 退出后挂起的协程
  状态不一致。现在设置停止标志，由 `Run()` 统一回收并复位。
- **异常只保留第一个且无定位信息**：现在记录失败总数与首个失败任务名。

自测见 `tests/batch_task_test.cc`，覆盖了上述每一项。

---

# 首先 Add 的时候发生了什么

你的三个任务：

```cpp
bt.Add(task1);
bt.Add(task2);
bt.Add(task3);
```

实际都会走

```cpp
void Add(Task t) {
    auto handle = t.Release();

    handle.promise().owner = this;
    ++active_tasks_;
    Schedule(handle);
}
```

注意这里：

```cpp
Schedule(handle);
```

只是

```cpp
ready_.push_back(handle);
```

也就是说

刚 add 完

```
ready_

+---------+
| task1   |
| task2   |
| task3   |
+---------+
```

**三个 coroutine 一个都没有开始执行。**

因为 promise 里面

```cpp
initial_suspend()
```

返回的是

```cpp
std::suspend_always
```

所以创建 coroutine 后立即 suspend。

---

# 然后 run()

进入

```cpp
bt.Run();
```

里面就是

```cpp
while (active_tasks_ > 0)
```

第一次循环：

```
ResumeReady();

destroy_finished();

MoveExpiredTimersToReady();
```

重点就是

```cpp
ResumeReady();
```

---

# 第一次 ResumeReady()

里面：

```cpp
while (!ready_.empty())
```

先拿

```
task1
```

然后

```cpp
handle.resume();
```

开始真正执行 coroutine。

---

# task1 开始执行

代码

```cpp
co_await bt.SleepFor(50ms);
```

来到

sleep_awaiter

发现

```cpp
await_ready()
```

返回

false

于是

```cpp
await_suspend()
```

执行

```cpp
owner.add_timer(expire_at, handle);
```

注意：

这里只做了一件事：

把 coroutine 放进 timer heap

```
timers

+----------------+
| task1 50ms     |
+----------------+
```

然后 coroutine suspend。

于是

```
task1
```

退出。

并没有继续往下

```cpp
write(pipe)
```

---

# scheduler 接着调 task2

继续：

```
ready

task2
```

resume

执行：

```cpp
co_await bt.Readable(pipe_fd[0]);
```

进入

```cpp
fd_awaiter
```

调用

```cpp
wait_fd(...)
```

里面：

```cpp
fd_waiters_[fd].push_back(...)
```

然后：

```cpp
epoll_ctl(ADD)
```

所以：

```
fd_waiters

pipe0

↓

task2
```

然后 coroutine suspend。

---

# scheduler 调 task3

resume：

开始：

```cpp
for (...)
```

它一直跑：

```
0

100000
```

到了：

```cpp
co_await bt.Yield();
```

这里非常关键。

yield 的 await_suspend：

```cpp
owner.Schedule(handle);
```

什么意思？

就是：

```
ready

task3
```

又放回 ready queue。

然后：

当前 coroutine suspend。

所以：

task3 没结束。

只是：

```
跑一点

↓

挂起

↓

重新排队
```

---

现在第一次

ResumeReady()

结束。

此时：

```
ready

task3
```

timer：

```
task1
```

fd：

```
task2
```

---

# ResumeReady() 会继续

因为：

```
ready

还有 task3
```

所以：

继续 resume。

task3 又继续：

```
100001

...

200000
```

再次：

```cpp
yield();
```

再次：

```
schedule(task3)
```

于是：

```
ready

task3
```

不断循环。

---

这里有一个很重要的特点：

你的

```cpp
ResumeReady()
```

不是：

```
resume 一个
```

而是：

```cpp
while (!ready.empty())
```

所以：

**它会一直执行所有 ready coroutine。**

因此：

task3 会一直：

```
resume

↓

yield

↓

resume

↓

yield
```

直到：

```
sum 算完
```

最后：

```
co_return
```

---

# co_return

来到：

```cpp
final_suspend()
```

里面：

```cpp
owner.Finish(handle)
```

于是：

```
destroy_later

task3
```

active--

---

现在：

ready 空了。

---

# run()

继续：

```cpp
MoveExpiredTimersToReady();
```

发现：

50ms

还没到。

于是：

```
ready

空
```

timer：

```
task1
```

fd：

```
task2
```

---

于是：

run()

来到：

```cpp
WaitEvents();
```

这里：

```
epoll_wait(timeout)
```

timeout 来自：

```cpp
NextTimeoutMs()
```

timer：

```
50ms
```

所以：

```
epoll_wait(...,50)
```

线程睡眠。

---

# 50ms 后

timer 到期。

epoll_wait timeout。

返回。

然后：

下一轮：

```cpp
MoveExpiredTimersToReady()
```

把：

```
task1
```

放进：

```
ready
```

---

resume：

task1

继续：

```cpp
write(pipe)
```

写：

```
hello
```

进入 pipe。

task1

结束。

---

下一轮：

因为：

pipe

已经可读。

所以：

```
epoll_wait
```

返回：

```
EPOLLIN
```

wait_events：

```cpp
schedule(task2)
```

于是：

```
ready

task2
```

---

resume：

task2

继续：

```cpp
read(pipe)
```

打印：

```
reader got:

hello from io task
```

然后：

结束。

---

所有：

```
active_tasks_

0
```

run()

退出。

---

# 整个调度时间线

```
add

ready:

task1
task2
task3

↓

run

↓

resume task1

↓

timer

↓

resume task2

↓

epoll wait

↓

resume task3

↓

yield

↓

resume task3

↓

yield

↓

...

↓

task3 finish

↓

epoll_wait(50ms)

↓

timer expire

↓

resume task1

↓

write(pipe)

↓

epoll readable

↓

resume task2

↓

read(pipe)

↓

finish
```

---

# 为什么 `yield()` 很重要？

如果去掉：

```cpp
co_await bt.Yield();
```

那么：

```cpp
for (int i = 0; i < 1000000; ++i)
```

会一直执行。

由于这是**单线程协程调度器**，调度器拿到 `task3` 后会一直执行到：

* `co_await`
* `co_return`
* 抛异常

才会切回来。

所以没有 `yield`：

```
task3

↓

一直算

↓

1,000,000

↓

结束
```

这期间：

* timer 不会检查
* epoll 不会处理
* task1 的 50ms 定时器即使到了也不会恢复
* task2 的 IO 事件也不会处理

整个 event loop 都会被 CPU 密集任务阻塞。

这也是像 `boost::asio`、`libuv`、`Tokio` 等协程/事件循环框架的共同原则：**协程是协作式调度（cooperative scheduling）**，不会被系统抢占。CPU 密集型任务必须主动 `co_await bt.Yield()`（或拆分成更小的任务），否则整个调度器都会失去响应。
