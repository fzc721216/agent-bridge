# MCP 接入

`agent-bridge mcp` 是标准 stdio MCP 服务，将当前成员的 `peers / ask / inbox / reply / request_status / publish_state / heartbeat` 暴露为工具。其内部通信是本地 `agent-bridge/1` 协议，未实现 A2A。支持 MCP 协议版本 `2024-11-05` 和 `2025-03-26`，不声明 resources、prompts、sampling 或 push 能力。

## 先建立接入身份

1. 启动 daemon，并由用户发现会话、创建通信组。
2. 使用 `invite` 为所选成员生成凭据文件。凭据绑定一个通信组和一个会话；再次邀请会轮换该身份，旧凭据失效。
3. 在该 agent 的会话里运行邀请说明中的 `join` 命令，实际完成握手。发送邀请不代表 agent 已经接入。
4. 用邀请输出的凭据文件路径配置 MCP 服务。文件中包含 token，保持文件仅当前用户可读；不要把 token 复制到命令行或对话中。

下面以安装后的程序路径和凭据路径为例。替换成 `invite` 输出的实际路径；非默认 state directory 还需要给 `agent-bridge` 加 `--state-dir` 或 `--socket`。

```bash
# 该会话先实际完成 CLI 握手
/data2/home/fuzicheng/agent-bridge/bin/agent-bridge \
  --credentials '/absolute/path/to/member-credentials.json' join

# Codex：注册 stdio MCP，命令参数逐个传递
codex mcp add agent-bridge -- \
  /data2/home/fuzicheng/agent-bridge/bin/agent-bridge \
  --credentials '/absolute/path/to/member-credentials.json' mcp

# Claude Code：同样注册 stdio MCP
claude mcp add --transport stdio agent-bridge -- \
  /data2/home/fuzicheng/agent-bridge/bin/agent-bridge \
  --credentials '/absolute/path/to/member-credentials.json' mcp
```

运行中的会话通常需要 reload MCP 或重新进入会话，工具才会出现。注册命令会修改对应客户端配置，项目不会自动执行这些命令。凭据属于具体成员：不同会话应使用自己的凭据和相应 MCP 配置，不能让全局配置中的同一凭据同时代表多个 agent。对于已经运行、无法重新加载 MCP 的会话，使用同一套带 `--credentials` 的 CLI 命令即可。

## 资料请求与回复

- 接入后先用 `publish_state` 声明当前任务和 `topics`，例如“支付需求”“数据库 schema”。`peers` 展示组内其他成员主动发布的资料范围，不读取它们隐藏的上下文。
- 缺资料时调用 `ask({"to":"codex:<id>","body":"请解释 order.status 的业务含义"})`。该调用立即返回请求信息，不等待另一 agent 工作结束。
- 继续能独立完成的工作，并通过 `request_status({"request_id":"<id>"})` 或 `inbox({})` 获取结果。
- 收到资料请求的成员调用 `reply({"request_id":"<id>","body":"...","sources":["/repo/schema.sql:42"]})`。来源是引用；broker 不会自动上传引用文件的内容。
- 信息不足时明确回复无法提供，不要递归把原请求转发给其他成员。不要回复“回复”或“心跳”，避免相互唤醒形成循环。
- `heartbeat` 只能表达接入身份仍可用。任务进展必须通过 `publish_state` 单独发布。

## 投递与能力边界

MCP 自身不唤醒 agent，也不会在后台自动调用 `inbox`；daemon 使用会话适配器投递新请求。CLI queue 或终端粘贴成功仅代表传输接受，agent 读取 inbox 才确认读取，实际提交 reply 才算回答。回复不额外触发自动唤醒；请求方应在工作间隙检查 inbox 或 request status。

当目标只能观察、被权限提示阻塞、没有完成 join 或通信已超时时，工具会返回可检查的错误或状态。`tools/call` 的执行失败通过标准 MCP `isError: true` 文本结果返回，参数错误通过 JSON-RPC `-32602` 返回。工具只能使用所配置的成员凭据，不能用参数覆盖 group 或 sender。
