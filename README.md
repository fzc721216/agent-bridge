# agent-bridge

用 Go 编写的本机 Codex / Claude Code 会话协作库、CLI 和终端看板。用户选择会话组成通信组；agent 主动声明正在做什么、掌握哪些资料，并向组内其他 agent 请求缺少的信息。

当前实现使用 **`agent-bridge/1` 本地协议，尚未实现 A2A**。MCP 是 agent 调用本程序工具的入口，也不是 A2A。A2A 的 Agent Card、任务生命周期和网络服务可以作为后续独立适配层；目前没有 A2A endpoint，也不宣称 A2A 兼容。

## 运行

需要 Go 1.22 或更新版本。Linux 可直接运行；macOS 支持 Unix socket 和进程探测，已做交叉编译检查，仍需在实际 macOS 主机上运行验证。SQLite 使用纯 Go 驱动，无需本机安装 SQLite 或 C 编译器。

```bash
cd /data2/home/fuzicheng/agent-bridge
go build -o bin/agent-bridge ./cmd/agent-bridge

# 终端一，前台运行 broker
./bin/agent-bridge daemon

# 终端二，只读发现会话，或进入多选看板
./bin/agent-bridge discover
./bin/agent-bridge tui

# 英文界面
./bin/agent-bridge tui --lang en
```

默认保存到 `~/.local/state/agent-bridge/`，尊重 `XDG_STATE_HOME`；也可在每次命令前指定 `--state-dir DIR`。默认 socket 位于该目录内，`--socket PATH` 可覆盖。daemon、SQLite 和凭据只对当前系统用户开放；这不是对同一系统用户下其他程序的隔离沙箱。

TUI 保留中文和英文，默认中文；`tui --lang zh|en` 指定初始语言，浏览页或确认页按 `l` 即时切换。状态、心跳和确认提示随语言切换；会话名称、文件路径及原始诊断信息保留原文。方向键移动，空格多选，`/` 过滤，`g` 输入组名并确认创建，`t` 切换 tmux 许可，Tab 切换会话/组，`i` 创建邀请，`d` 在邀请确认页切换自动投递，`y` 确认、Esc 取消、`q` 退出。自动邀请会调用目标 agent，因此必须在确认页明确启用。

## 从发现到资料请求

先从 `discover` 选择实际 ID。以下命令中的 `GROUP`、`CODEX_ID`、`CLAUDE_ID` 和凭据路径需要替换。

```bash
./bin/agent-bridge group create dev \
  --members 'codex:CODEX_ID,claude:CLAUDE_ID'
./bin/agent-bridge group list

# 为成员生成邀请说明和私有凭据；默认不往 agent 会话发消息。
./bin/agent-bridge invite GROUP codex:CODEX_ID
./bin/agent-bridge invite GROUP claude:CLAUDE_ID

# 确认适配器可投递时，可选择发送邀请；自动化调用可加 --yes。
./bin/agent-bridge invite GROUP codex:CODEX_ID --deliver
```

把邀请输出的 `prompt` 交给对应 agent。**agent 自己实际执行 `join` 才算接入**，邀请发出或队列接受都不代表握手成功。已有会话可直接使用邀请中的 CLI，无需重启；使用 MCP 工具则需要各自加载配置，见 [MCP 接入](docs/MCP.md)。再次邀请会轮换凭据，旧凭据会失效。

agent A 的操作示例：

```bash
./bin/agent-bridge --credentials /path/to/a.json join --topics '接口实现,错误处理'
./bin/agent-bridge --credentials /path/to/a.json state \
  --status working --task '实现订单接口' --summary '需要确认状态字段含义'
./bin/agent-bridge --credentials /path/to/a.json peers
./bin/agent-bridge --credentials /path/to/a.json ask \
  --to claude:CLAUDE_ID --body 'order.status 的取值和业务含义是什么？'
```

`ask` 立即返回 `message_id`，该 ID 就是后续的请求 ID。agent B 在接入后处理：

```bash
./bin/agent-bridge --credentials /path/to/b.json inbox
./bin/agent-bridge --credentials /path/to/b.json reply \
  --request-id REQUEST_ID --body '0=待支付，1=已支付，2=已取消' \
  --sources '/repo/docs/order.md:42,/repo/schema.sql:10'
```

agent A 使用 `inbox` 获取回答正文和来源，或用 `request-status REQUEST_ID` 查看请求是否已回答。来源是文件/URL 引用；文件内容需要回答者明确放进回答中，broker 不自动读取或上传引用文件。

请求与普通消息触发一次目标会话通知；回复、心跳和状态更新不会自动再唤醒会话，避免回复循环。请求方应在独立工作间隙检查 inbox。目标仅能观察时仍可通过主动轮询 inbox 协作，但不会凭空收到推送。agent 是否遵循协作说明、何时检查消息，取决于实际会话和工具权限。

## 会话能力和边界

| 接入方式 | 发现 | 发送通知 | 状态来源 |
|---|---|---|---|
| 已有 Codex | 只读元数据、活跃线程锁、daemon 已加载线程 | CLI 支持时使用 `codex queue` | provider 活跃证据 + agent 主动发布 |
| 已有 Claude | `claude agents --json`、会话元数据、PID/进程代次核对 | tmux 中且确认空闲、明确允许注入时可投递 | provider 元数据 + agent 主动发布 |
| 普通终端/IDE 中不能投递的会话 | 仍可列出 | `observe-only`，可用 CLI/MCP 主动拉取 | agent 主动发布 |
| 本程序启动的会话 | daemon 管理 | Codex app-server / Claude stream-json | 公开 provider 事件 + agent 主动发布 |

历史会话不会因“最近修改过”而被标为活跃。程序不会读取其他 agent 隐藏上下文或思考内容，也不调用 Claude 私有 `messagingSocketPath` 协议。

如需由 broker 管理会话：

```bash
./bin/agent-bridge start --provider codex --cwd /path/to/repo --name api
./bin/agent-bridge start --provider claude --cwd /path/to/repo --name requirements
```

这些会话没有独立交互终端，通过 `invite --deliver`、`send` 和协作工具通信。provider 模型沿用用户现有配置，不自动绕过工具权限；需要人工批准的操作会被拒绝并显示阻塞。daemon 退出会停止自己启动的子进程，重启后不会恢复内存中的进程句柄，也不会接管或停止独立启动的会话。

## 投递与状态语义

- `state=queued`：请求已持久化；不证明 agent 看到了消息。
- `delivery=sending/accepted`：正在调用传输/传输接受通知；不证明 agent 已阅读。
- `state=read`：agent 调用了 inbox，确认取到了消息。
- `state=answered`：原接收者提交了关联回复。
- `expired/failed`：过期或成员移除等终止原因。通知失败保留独立的 `delivery` 和错误字段，消息仍可由 inbox 获取。
- daemon 崩溃时仍处于 `sending` 的投递恢复为 `unknown`；不盲目重发可能已经进入 provider 队列的通知。
- 心跳只表示通信仍在活动。任务状态和摘要由 agent 显式发布；两分钟没有心跳会显示 stale，心跳不会把旧任务误恢复为 working。

默认请求 TTL 为 15 分钟，最大 24 小时；正文上限 64 KiB，收件箱有容量和分页限制。可用 `--idempotency-key KEY` 避免同一发送者重复创建相同请求。通信组、令牌散列、消息和事件保存于 SQLite；消息正文留在本地数据库中。

## 作为 Go 库使用

根包 `bridge` 提供持久化 broker；`adapters` 提供发现/发送，`managed` 管理自有子进程，`local` 提供 Unix socket API，`mcp` 和 `tui` 提供入口。模块路径目前仅用于本地开发，尚未发布到远程仓库或包平台。

```go
ctx := context.Background()
manager := adapters.New(adapters.Options{})
sessions, warnings := manager.Discover(ctx)

broker, err := bridge.Open("/private/path/bridge.sqlite")
if err != nil { return err }
defer broker.Close()
if err := broker.UpsertSessions(ctx, sessions); err != nil { return err }
// 由调用方让用户选择 sessions，再调用 CreateGroup / Invite / Join / Send。
_ = warnings
```

根包的管理 API 面向本机所有者；嵌入到其他服务时须像 `local.Server` 一样先校验成员凭据，并限制 sender/group，不能将管理 API 直接暴露给远程客户端。

## 验证

```bash
go test ./...
go test -race ./...
go vet ./...
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o bin/agent-bridge-darwin-arm64 ./cmd/agent-bridge
```

测试使用 fixture 和模拟的 provider 可执行文件，不调用真实模型。真实会话仅做只读发现；消息的真实 agent 理解和自动回复需要用户选择并实际接入会话后验证。验证结果见 [验证记录](docs/VALIDATION.md)。

参考：[A2A 规范](https://a2a-protocol.org/latest/specification/)、[A2A Go SDK](https://github.com/a2aproject/a2a-go)、[OpenAI Docs：Codex app-server](https://developers.openai.com/codex/app-server)。
