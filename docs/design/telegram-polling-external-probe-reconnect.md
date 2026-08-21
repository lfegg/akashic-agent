# Telegram polling 外部探活与自动重连

> 状态：已实现。
> 关联文件：`infra/channels/telegram_channel.py`、`tests/test_channel_clients.py`。
> 事故背景：2026-08-20，外部网络中断后 Telegram polling 长时间假死；进程和
> Web 通道仍然正常，但 Telegram 无法继续收发。

## 1. 问题

Telegram 是由本机主动连接 `api.telegram.org` 的长轮询通道。网络或代理连接中断后，
python-telegram-bot（PTB）会在自己的 `network_retry_loop(max_retries=-1)` 中重试。
现有 Core 只能观察 error callback，无法通知正在退避或卡住的 polling task“外部路由已
恢复”。

原有 Conflict 处理也不能解决这个问题：409 Conflict 属于 PTB 已知错误，应该继续由
PTB 原生退避处理。故障样本中的 `RemoteProtocolError`、`ConnectError` 和后续长时间
无 polling 活动属于不同故障面。

仅检查 `updater.running` 也不够。PTB 的内部 polling task 可能已经结束，或 task 仍未
结束但连接池已经不再产生有效活动；两种情况下 updater 都可能继续表现为 running。

## 2. 目标

实现两条彼此独立的恢复轨道：

1. polling watchdog 发现 PTB polling task 已结束或缺失时，复位 updater 并重启；
2. 外部连通性探活发现网络由不可用恢复为可用，同时 polling task 仍存活但长期没有可
   观察活动时，强制复位 polling。

正常 PTB 重试、正常关闭和短暂网络抖动不得触发额外重启。

## 3. 状态与参数

| 参数 | 默认值 | 作用 |
| --- | ---: | --- |
| `_POLLING_WATCH_INTERVAL_SECONDS` | 15 | polling task watchdog 周期 |
| `_POLLING_RESTART_BASE_DELAY` | 2 | watchdog 首次重启退避 |
| `_POLLING_RESTART_MAX_DELAY` | 60 | watchdog 最大重启退避 |
| `_PROBE_INTERVAL_SECONDS` | 30 | 独立外部探活周期 |
| `_PROBE_TIMEOUT_SECONDS` | 5 | 单次探活超时 |
| `_PROBE_FAIL_THRESHOLD` | 3 | 进入网络不可用状态所需连续失败数 |
| `_STALL_THRESHOLD_SECONDS` | 120 | polling 无可观察活动的假死阈值 |

通道维护以下运行状态：

- `_last_poll_activity_at`：最近一次成功完成 `getUpdates`（包括空长轮询）、收到 update、
  收到 polling error，或成功重启 polling 的 monotonic 时间；
- `_last_probe_ok`：最近一次独立探活是否成功；
- `_probe_consecutive_fail`：连续探活失败次数；
- `_last_probe_ok_at`：最近一次探活成功时间；
- `_probe_network_unavailable`：连续失败达到阈值后置为 true；
- `_shutting_down`：正常关闭 fence；
- `_polling_restart_lock`：串行化 watchdog 和 probe 的 updater 复位操作。

独立探活成功本身不会更新 `_last_poll_activity_at`。否则探活会把自己伪装成 polling
活动，使 stall 判定永远无法成立。成功的空 `getUpdates` 会更新时间戳，因此健康的
长轮询不会因长期没有用户消息而被误判为假死。

## 4. 独立外部探活

`_connectivity_probe_loop` 每 30 秒创建一个独立的 `httpx.AsyncClient`，使用：

```python
httpx.AsyncClient(
    timeout=5,
    trust_env=True,
    follow_redirects=False,
)
```

请求目标是 `https://api.telegram.org` 根地址，而不是 `/bot<TOKEN>/getMe`。选择根地址
有两个原因：

1. 本机制只负责判断到 Telegram 的外部路由是否恢复；token 有效性继续由 PTB 初始化和
   polling 负责；
2. 独立 HTTP 客户端及其日志中不会出现 bot token。

`trust_env=True` 让每轮新 client 重新读取代理环境。HTTP 5xx 和 `httpx.HTTPError` 记为
失败；其他 HTTP 响应证明 Telegram 路由可达。

探活失败在达到阈值前仅记录 info。第三次连续失败进入 network unavailable 状态并记录
warning。只有从该状态转为成功才进入恢复判定；单次或两次抖动不会触发恢复动作。

## 5. 恢复判定

探活从 unavailable 转为成功时执行 `_recover_stalled_polling`：

1. `_shutting_down` 为 true 或 updater 已停止：直接返回；
2. polling task 缺失或已经 done：不由 probe 重启，交给 polling watchdog；
3. polling task 仍存活且最近 polling 活动距今小于 120 秒：认为 PTB 已自行恢复，不重启；
4. polling task 仍存活且超过 120 秒无活动：判定假死，调用 `_restart_polling`。

因此真正触发 probe 重启必须同时满足：

```text
连续外部探活失败达到阈值
AND 探活随后恢复
AND updater 仍为 running
AND polling task 仍未结束
AND polling 可观察活动超过 stall 阈值
AND 通道不在 shutdown
```

## 6. Polling watchdog

`_watch_polling_loop` 独立检查 PTB 22.x 的 `_Updater__polling_task`。当 updater 仍为 running，
但 task 缺失、被意外取消、无异常提前结束或异常退出时，watchdog 记录原因并按指数退避
调用 `_restart_polling`。如果后续 PTB 版本不再提供该私有状态，watchdog 会降级为只记录并
跳过恢复，不把“字段不可用”误判为 polling task 已死亡。

这是 probe 恢复轨道的互补路径：

- task 已结束：watchdog 负责；
- task 未结束但连接疑似假死：probe 恢复轨道负责；
- task 正常且持续有 update/error 活动：两者都不干预。

## 7. 重启与关闭所有权

`_restart_polling` 接收触发恢复时观察到的 polling task 身份，并在
`_polling_restart_lock` 内执行：

1. 再次检查 `_shutting_down` 和 `updater.running`；
2. 若当前 polling task 已不是触发恢复时观察到的 task，说明另一条恢复轨道已经换代，
   直接返回，避免串行执行第二次重启；
3. 调用 `updater.stop()` 清理 PTB 的 running 状态和旧 task；若 PTB 因旧 task 已经
   失败而从 `stop()` 传播原异常，则清理失败 task、stop event 和 cleanup callback 后继续
   复位；
4. 再次检查 shutdown fence；
5. 使用原 `allowed_updates` 和 `_on_polling_error` 调用 `start_polling()`；
6. 记录新的 polling 活动时间。

`stop()` 先设置 `_shutting_down=True`，再取消并等待 watchdog 与 probe 任务，最后停止
updater 和 Application。因此后台恢复任务不能跨越正常关闭重新拉起 polling。

## 8. 日志

关键日志包括：

- `[telegram] 外部探活失败（连续 N 次）: <error type>`；
- `[telegram] 外部探活恢复`；
- `[telegram] polling task 存活但持续无网络活动 N.s，判定假死`；
- `[telegram] 外部探活恢复，重启 polling`；
- `[telegram] polling 守护已自动重启轮询`。

网络错误只记录异常类型，不记录带 token 的请求 URL。

## 9. 保持不变

- 不修改 PTB 的 native `network_retry_loop(max_retries=-1)`；
- 不修改 409 Conflict 的原生退避所有权；
- 不引入代理软件或操作系统路由监听；
- 不在探活中读写主配置或 workspace；
- 不影响 Web、MCP 或其他 channel 生命周期。

## 10. 测试覆盖

`tests/test_channel_clients.py` 固定以下行为：

- start/stop 成对持有并清理 watchdog 和 probe task；
- 探活 client 使用 `trust_env=True`，请求 URL 不含 token；
- 连续失败未达到阈值时不重启；
- 达到阈值、恢复且 polling stale 时重启；
- polling 最近有活动时不重启；
- 空 `getUpdates` 成功返回会被计为 polling 活动；
- polling task 已结束时交给 watchdog；
- watchdog 能重启异常结束的 task；
- `updater.stop()` 传播失败 task 异常时仍能清理并启动新 polling；
- shutdown fence 阻止任何重新拉起；
- 原 Conflict callback 仍然只观测，不停止 updater。

## 11. 真实验收

自动化测试覆盖状态机和生命周期。真实网络验收仍需在运行 bot 的机器上执行：

1. 关闭外部网络或代理，确认连续探活失败达到阈值；
2. 保持超过 stall 阈值后恢复网络；
3. 确认出现恢复与 polling 重启日志；
4. 从 Telegram 发送消息，确认正常收取和回复；
5. 确认进程、Web 和其他通道没有重启或中断。

## 12. 本机验证（2026-08-20）

- Channel、host、adapter、Telegram utility 与 Web channel 回归：`122 passed`；
- `compileall`：通过；
- 使用实现中的独立 probe 真实访问 `https://api.telegram.org`：`HTTP 302`；
- 尚未自动关闭并恢复系统代理，因此完整断网恢复场景仍保留为 live 验收项。
