# 验证记录

日期：2026-11-19（Asia/Shanghai）。环境：Linux amd64，本机 Go 工具链；工程目标版本为 Go 1.22。

| 检查 | 结果 | 证据和范围 |
|---|---|---|
| `go test ./...` | 通过 | broker、adapters、CLI、local IPC、managed、MCP、TUI 共 7 个包 |
| `go test -race ./...` | 通过 | 同上，包括 SQLite 并发发送、管道读写、进程退出和令牌更新 |
| `go vet ./...` | 通过 | 全工程静态检查 |
| `gofmt -l .` | 无输出 | 全部 Go 文件符合格式 |
| Linux CLI 构建 | 通过 | `bin/agent-bridge` |
| macOS arm64 交叉编译 | 通过 | `CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build`；未声称已做 macOS 实机运行 |
| 真实会话只读发现 | 通过 | 当次发现 505 个 Codex 记录、6 个 Claude 记录；确认 1 个活跃 Codex，其余为 observe-only；无 provider 诊断错误 |
| daemon 生命周期 | 通过 | 启动、状态读取、重复启动拒绝、SIGTERM 正常退出、socket 清理；不会覆盖已有普通文件 |
| 权限 | 通过 | 实测数据库和 Unix socket 均为 0600；凭据文件测试验证为 0600 |
| TUI 伪终端检查 | 通过 | 渲染选择页面，`q` 正常退出，进程返回 0；选择和确认行为另有模型单测 |
| 完整模拟资料请求 | 通过 | `python3 scripts/smoke.py`，见下文 |

## 资料请求的完整模拟

演示脚本在临时目录创建两个模拟 provider 可执行文件，并将 provider home 指向空目录。模拟进程实现真实 app-server / stream-json 管道格式；其“agent”行为由固定脚本提供，不调用模型。

测试使用构建好的 CLI 完成以下流程：

1. 启动 daemon 和两个受控模拟会话：Codex 负责接口、Claude 负责 schema。
2. 用户通过 CLI 明确选择这两个会话创建通信组。
3. 投递邀请，两个模拟 agent 实际执行带各自凭据的 `join` 和状态发布命令。
4. Codex 身份提交“字段含义是什么”的 `ask`，立即拿到请求 ID。
5. daemon 通过 provider 管道通知 Claude；模拟 Claude 主动调用 inbox，随后提交关联 reply 和 `schema.sql:12` 来源。
6. 请求方看到 `answered`，inbox 中收到正文和来源；回复保持 inbox-only，不继续自动唤醒。
7. 正常停止并重启 daemon，验证组、凭据、已回答请求和回复仍可读取。

输出：

```text
PASS: managed Codex/Claude -> selected group -> invitations -> real join -> ask -> notify -> reply with source
PASS: daemon restart retains group, scoped credentials, answered request and reply
```

## 实际验证的边界

- 未向本机既有 agent 会话发送消息，未修改用户的 Codex/Claude/MCP 配置。
- 未调用真实模型；因此尚未验证真实模型是否遵循协作说明、是否及时检查 inbox，以及实际工具权限是否允许执行协作命令。
- tmux 使用 fake executable 和进程 fixture 验证选择、状态核对、字面量投递及拒绝条件；没有在真实 Claude tmux pane 中投递消息。
- 独立启动的 Claude 无公开通用投递入口时显示 observe-only；其成员仍可通过主动 CLI/MCP 拉取消息参与。
- A2A 兼容性不在本版本声明范围内；当前协议为 `agent-bridge/1`，MCP 服务为 stdio 工具入口。
