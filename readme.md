遇到的问题：
## 问题1
按引用捕获方式去捕获指针，结果运行时出现段错误，类成员函数指针是无效值
template<typename C, typename R, typename... Args>
void register_function(const std::string& name, std::shared_ptr<C> obj, R(C::*mem)(Args...)) {
    std::function<R(Args...)> fn = [obj, &mem](Args... args) -> R {
        return (obj->*mem)(std::forward<Args>(args)...);
    };

    handlers[name] = [fn](const json& params) -> json {
        R result = call_with_json(fn, params);
        return json{{"result", result}};
    };
}

//mem应该使用值捕获，不然超出函数作用域会失效
template<typename C, typename R, typename... Args>
void register_function(const std::string& name, std::shared_ptr<C> obj, R(C::*mem)(Args...)) {
    std::function<R(Args...)> fn = [obj, mem](Args... args) -> R {
        return (obj->*mem)(std::forward<Args>(args)...);
    };

    handlers[name] = [fn](const json& params) -> json {
        R result = call_with_json(fn, params);
        return json{{"result", result}};
    };
}

## 问题2
signal(SIGTSTP,func)   func不能绑定类成员函数，只能绑定类外函数，或者类静态成员函数
为了让某个信号到来时调用类的成员函数，使用sigwait函数，sigwait函数可以事先指定一些信号，sigwait一直阻塞到这些信号到来，根据到来的信号类型，执行相应的操作
实现点击ctrl+z时，关闭打开的套接字资源







下面用**阶段→消息→流程→实战例子**的方式，把 MapReduce 中 **Coordinator（协调者）** 与 **Worker（执行者）** 的交互讲清楚。示例采用经典的 **词频统计（WordCount）**，并展示从任务分配到最终输出的完整链路。

---

## 一、典型交互阶段（Coordinator ↔ Worker）

> 约定：Map 任务数 = `M`，Reduce 分区数 = `R`。
> Worker 既可执行 Map 也可执行 Reduce（很多实现用同一进程切换角色）。

1. **Worker 注册 / 心跳**

* **注册**：Worker 启动后向 Coordinator 报到（Register），获取集群参数（R 值等）。
* **心跳**：周期性汇报存活状态和当前任务进度，Coordinator 用于**失败检测**与**任务重派**。

2. **任务请求 / 分配（Map 阶段）**

* Worker 发送：`RequestTask`（空闲可领活）
* Coordinator 发送：`TaskAssignment{ type=MAP, input=文件片段, mapID }`
* 若无可分配任务，返回 `Sleep/NoTask`，Worker 休眠后重试

3. **Map 执行 / 中间结果写盘（本地）**

* Worker 运行用户 `Map(key, value)`（通常 `key=文件名/偏移`，`value=内容`）
* 产生的 `(k, v)` 中间键值对按 `partition = hash(k) % R` 分桶
* **落盘**为临时文件：`mr-mapID-reduceID`（如 `mr-7-1`）
* 可选 Combiner：在 Map 端对局部相同 key 先聚合，减少网络量

4. **Map 完成汇报**

* Worker 发送：`ReportTaskDone{ mapID, outputs=[(reduceID, filePath)...] }`
* Coordinator 标记该 Map 完成，并记录**每个 reduceID 对应有哪些 map 中间文件**

5. **阶段转换：从 Map → Shuffle/Reduce**

* Coordinator 等 **所有 Map 完成**，再宣布 Reduce 阶段可开始

6. **任务请求 / 分配（Reduce 阶段）**

* Worker 发送：`RequestTask`
* Coordinator 发送：`TaskAssignment{ type=REDUCE, reduceID }`

7. **Shuffle 拉取 / Merge / Reduce 执行**

* Reduce Worker 根据 `reduceID`：

  * 从**各个 Map Worker 节点**拉取对应的 `mr-*-reduceID` 文件（RPC/HTTP/本地拷贝）
  * **合并 & 按 key 排序 / 分组**
  * 对每个 key 的 value 列表运行用户 `Reduce(key, [v...])`
  * 写出最终文件：`mr-out-reduceID`

8. **Reduce 完成汇报 / 作业结束**

* Worker 发送：`ReportTaskDone{ reduceID, outputFile }`
* 当 **所有 Reduce 完成**，Coordinator 宣布 Job 成功结束

9. **失败重试（任一阶段可能发生）**

* Coordinator 发现某 Worker 心跳超时或任务超时，**将该任务标记为未完成并重派**给其他 Worker
* Map 任务重跑会生成新的临时文件；Reduce 任务重跑会重新拉取并计算

---

## 二、常见消息/RPC（伪接口）

* `Register(workerId, addr) -> {R, jobMeta}`
* `Heartbeat(workerId, runningTask?, progress?) -> ack`
* `RequestTask(workerId) -> TaskAssignment{MAP/REDUCE/NoTask}`
* `ReportTaskDone(workerId, taskInfo)`
* `FetchIntermediate(srcAddr, fileName) -> file bytes`（Reduce 从 Map 端拉取文件）

> 实现上可以是 gRPC/HTTP/RPC/消息队列；教学实现多用简单 RPC 或 HTTP+文件。

---

## 三、实战例子：WordCount（M=3, R=2）

### 输入

* `a.txt`: `to be or not to be`
* `b.txt`: `be the change you wish`
* `c.txt`: `to change the world`

### 目标

统计每个单词出现次数。

### 1) Map 阶段

* Coordinator 把输入拆成 3 个 Map 任务：`mapID=0(a.txt), 1(b.txt), 2(c.txt)`
* 三个 Worker 并行领取并执行 `Map`，示意输出（去标点、小写化）：

`map0(a.txt)` 产出：

```
(to,1) (be,1) (or,1) (not,1) (to,1) (be,1)
```

`map1(b.txt)` 产出：

```
(be,1) (the,1) (change,1) (you,1) (wish,1)
```

`map2(c.txt)` 产出：

```
(to,1) (change,1) (the,1) (world,1)
```

**分区**：`reduceID = hash(word) % 2`
（为便于演示，这里假设 hash 结果让如下分配成立）

* 落盘示例（每个 Map 对两个 reduceID 均可能有文件）：

  * `map0`：`mr-0-0` 含 {be, not}；`mr-0-1` 含 {to, or}
  * `map1`：`mr-1-0` 含 {be, you}；`mr-1-1` 含 {the, change, wish}
  * `map2`：`mr-2-0` 含 {the, world}；`mr-2-1` 含 {to, change}

> **Combiner（可选）**：Map 端可先把同一文件内相同词做局部合并，如 `map0` 把 `(to,1)(to,1)` 合成 `(to,2)`，再按分区写盘，能显著减少中间对数量。

Map Worker 完成后向 Coordinator 报告文件清单，比如：

```
map0 done: [ (0, "mr-0-0"), (1, "mr-0-1") ]
map1 done: [ (0, "mr-1-0"), (1, "mr-1-1") ]
map2 done: [ (0, "mr-2-0"), (1, "mr-2-1") ]
```

### 2) Shuffle + Reduce 阶段

* 所有 Map 完成后，Coordinator 开放 Reduce 任务：`reduceID=0,1`
* 两个 Reduce Worker 并行领取

**Reduce 0** 拉取：`mr-0-0, mr-1-0, mr-2-0`

* 合并分组后可能得到（示意）：

  * `be: [1,1,1]`（来自 a,b）
  * `not: [1]`
  * `you: [1]`
  * `the: [1,1]`（来自 b,c）
  * `world: [1]`
* 执行 `Reduce(k, vs)` 求和并写出 `mr-out-0`，内容示意：

  ```
  be 3
  not 1
  the 2
  you 1
  world 1
  ```

**Reduce 1** 拉取：`mr-0-1, mr-1-1, mr-2-1`

* 分组：

  * `to: [1,1,1]`（a,c）
  * `or: [1]`
  * `change: [1,1]`（b,c）
  * `wish: [1]`
* 写出 `mr-out-1`：

  ```
  change 2
  or 1
  to 3
  wish 1
  ```

### 3) 作业完成 & 汇总

* 两个 Reduce 都上报完成，Coordinator 宣布 Job 成功
* （可选）后处理把 `mr-out-0` 与 `mr-out-1` 合并为一个总输出文件

**最终词频：**

```
be:3  change:2  not:1  or:1  the:2  to:3  wish:1  world:1  you:1
```

---

## 四、失败与重试（快速示例）

* 若 `map1` 的 Worker 崩溃：

  * Coordinator 心跳/超时检测到，**将 map1 标记未完成并重派**到其他空闲 Worker
  * 新 Worker 重新执行 `b.txt` 的 Map，生成新的 `mr-1-*` 文件；旧文件（若不完整）可忽略
* 若 `reduce0` 崩溃：

  * Coordinator 重派 `reduceID=0`，新的 Reduce Worker **重新拉取**所有 `mr-*-0` 并计算

MapReduce 借助**幂等**与**纯函数式接口（Map/Reduce 不依赖外部可变状态）**，让重试更容易正确。

---

## 五、两个核心循环（伪代码）

**Coordinator 主循环**

```pseudo
while job_not_done:
  for each worker:
    if heartbeat_timeout(worker):   // 失败检测
      if worker.running_task:
        mark_as_pending(worker.running_task)  // 重派
  for each idle_worker:
    if pending_map_exists:
      assign_map_task(idle_worker)
    else if all_maps_done and pending_reduce_exists:
      assign_reduce_task(idle_worker)
  if all_reduces_done: break
  sleep(tick)
```

**Worker 主循环**

```pseudo
register()
loop:
  send_heartbeat()
  task = request_task()
  if task == NoTask: sleep(backoff); continue
  if task.type == MAP:
     run_map(task.input)
     write mr-mapID-reduceID files
     report_map_done(files)
  else if task.type == REDUCE:
     files = list_all_mr_star_reduceID()
     fetch_and_merge(files)
     sort_group_by_key()
     run_reduce_per_key()
     write mr-out-reduceID
     report_reduce_done(out)
```

---

### 小结

* **交互阶段**：注册/心跳 → Map 分配与执行 → 中间文件分区/落盘 → Map 完成 → Reduce 分配 → Shuffle 拉取/合并/排序 → Reduce 执行 → 输出 → 结束
* **关键点**：

  * Map 端**按 key 分区**，Reduce 端**按 key 分组**
  * Coordinator 负责**调度、状态、超时重试**
  * 失败重试靠**幂等性**与**中间文件的可再用性**

如果你需要，我可以把上面的流程直接落成一个**最小可运行 C++ 多线程 Demo**（含中间文件命名、模拟 RPC/HTTP 的本地拷贝、失败重试的超时机制）。你更偏好“单文件最小化”还是“多文件模块化”？我按你的偏好给代码。
