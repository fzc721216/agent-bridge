package tui

import (
	"fmt"
	"strings"

	bridge "github.com/fuzicheng/agent-bridge"
)

// Language controls presentation only. Provider and protocol values stay intact.
type Language string

const (
	Chinese Language = "zh"
	English Language = "en"
)

// ParseLanguage accepts the language tags used by the CLI. Chinese is the default.
func ParseLanguage(value string) (Language, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", "-")) {
	case "", "zh", "zh-cn", "cn":
		return Chinese, nil
	case "en", "en-us":
		return English, nil
	default:
		return "", fmt.Errorf("unsupported TUI language %q (use zh or en)", value)
	}
}

func (m model) tr(english string) string {
	if m.language == English {
		return english
	}
	if chinese, ok := chineseText[english]; ok {
		return chinese
	}
	// Callers may include layout newlines around a message; preserve those while
	// sharing the same catalog entry with notice and error text.
	key := strings.Trim(english, "\n")
	if chinese, ok := chineseText[key]; ok {
		start := len(english) - len(strings.TrimLeft(english, "\n"))
		end := len(english) - len(strings.TrimRight(english, "\n"))
		return english[:start] + chinese + english[len(english)-end:]
	}
	return english
}

func (m model) statusText(status bridge.Status) string {
	if status == "" {
		status = bridge.Unknown
	}
	return m.tr(string(status))
}

func (m model) transportText(transport bridge.Transport) string {
	if transport == "" {
		return m.tr("unknown")
	}
	return m.tr(string(transport))
}

func (m model) boolText(value bool) string {
	if value {
		return m.tr("Yes")
	}
	return m.tr("No")
}

func (m model) eventText(event string) string {
	if m.language == English {
		return event
	}
	if text, ok := chineseEvents[event]; ok {
		return text
	}
	return event
}

var chineseText = map[string]string{
	"agent-bridge  •  local agent-bridge/1 protocol":                                                                           "agent-bridge  •  本地 agent-bridge/1 协议",
	"Tab panel · ↑/↓ move · Space select · / filter · g create · t tmux consent · i invite · r discover · q quit":              "Tab 切换区域 · ↑/↓ 移动 · 空格选择 · / 筛选 · g 建组 · t 允许 tmux · i 邀请 · r 探测 · q 退出",
	"Tab panel · ↑/↓ move · Space select · / filter · g create · t tmux consent · i invite · r discover · l language · q quit": "Tab 切换区域 · ↑/↓ 移动 · 空格选择 · / 筛选 · g 建组 · t 允许 tmux · i 邀请 · r 探测 · l 语言 · q 退出",
	"Filter: %s   Selected: %d   tmux delivery consent for new group: %t":                                                      "筛选：%s   已选：%d   新通信组允许 tmux 投递：%t",
	"Filter: %s   Selected: %d   tmux delivery consent for new group: %s":                                                      "筛选：%s   已选：%d   新通信组允许 tmux 投递：%s",
	"Group name: %s█  (Enter review, Esc cancel)":                                                                              "通信组名称：%s█  （Enter 查看确认，Esc 取消）",
	"Type filter, Enter done, Esc back":                                                                                        "输入筛选条件，Enter 完成，Esc 返回",
	"Create group %q with these members?":                                                                                      "使用以下成员创建通信组 %q？",
	"tmux message injection allowed: %t\nPress y to create, n/Esc to cancel.":                                                  "允许通过 tmux 注入消息：%t\n按 y 创建，n/Esc 取消。",
	"tmux message injection allowed: %s\nPress y to create, n/Esc to cancel.":                                                  "允许通过 tmux 注入消息：%s\n按 y 创建，n/Esc 取消。",
	"Invite all members of group %s? Existing credentials will be rotated.":                                                    "邀请通信组 %s 的全部成员？已有凭据将被替换并失效。",
	"  %s  transport=%s  joined=%t":                                                                                            "  %s  通信方式=%s  已接入=%t",
	"  %s  transport=%s  joined=%s":                                                                                            "  %s  通信方式=%s  已接入=%s",
	"Deliver invitations through provider/tmux: %t (d toggles).\nPress y to write credentials%s, n/Esc to cancel.":             "通过会话接口/tmux 投递邀请：%t（按 d 切换）。\n按 y 写入凭据%s，n/Esc 取消。",
	"Deliver invitations through provider/tmux: %s (d toggles).\nPress y to write credentials%s, n/Esc to cancel.":             "通过会话接口/tmux 投递邀请：%s（按 d 切换）。\n按 y 写入凭据%s，n/Esc 取消。",
	" and deliver notifications":                                                                                               "并发送通知",
	" only":                                                                                                                    "，仅写入",
	"> SESSIONS":                                                                                                               "> 会话",
	"  SESSIONS":                                                                                                               "  会话",
	"SESSIONS":                                                                                                                 "会话",
	"> GROUPS":                                                                                                                 "> 通信组",
	"  GROUPS":                                                                                                                 "  通信组",
	"GROUPS":                                                                                                                   "通信组",
	"saved":                                                                                                                    "历史",
	"live":                                                                                                                     "活跃",
	"idle":                                                                                                                     "空闲",
	"working":                                                                                                                  "处理中",
	"blocked":                                                                                                                  "受阻",
	"done":                                                                                                                     "已完成",
	"stale":                                                                                                                    "已过期",
	"unknown":                                                                                                                  "未知",
	"observe-only":                                                                                                             "仅观察",
	"codex-queue":                                                                                                              "Codex 队列",
	"tmux":                                                                                                                     "tmux",
	"managed":                                                                                                                  "受控会话",
	"No sessions. Press r to discover; daemon warnings appear below.":   "暂无会话。按 r 重新探测；后台服务提示显示在下方。",
	"  No sessions. Press r to discover; daemon warnings appear below.": "  暂无会话。按 r 重新探测；后台服务提示显示在下方。",
	"  %d sessions total; scroll to see more":                           "  共 %d 个会话，可上下移动查看更多",
	"No groups. Select sessions, then press g.":                         "暂无通信组。选择会话后按 g 创建。",
	"  No groups. Select sessions, then press g.":                       "  暂无通信组。选择会话后按 g 创建。",
	"needs-join":                      "待接入",
	"joined":                          "已接入",
	"no heartbeat":                    "尚无心跳",
	"Not connected: waiting for join": "尚未接入：等待 agent 执行 join",
	"No heartbeat received":           "尚未收到心跳",
	"Last heartbeat: %s ago":          "上次心跳：%s前",
	"heartbeat %s ago":                "上次心跳：%s前",
	"%ds":                             "%d秒",
	"%dm %ds":                         "%d分%d秒",
	"%dh %dm":                         "%d小时%d分",
	"    %s; knows: %s":               "    %s；掌握资料：%s",
	"Recent group events (delivery accepted does not imply read):": "最近通信组事件（投递已接受不代表已阅读）：",
	"Working…":                     "正在处理…",
	"Error: %s":                    "错误：%s",
	"Warning: %s":                  "提示：%s",
	"invitation for %s failed: %w": "邀请 %s 失败：%w",
	"notification accepted; waiting for join":          "通知已接受，等待 agent 接入",
	"not delivered: ":                                  "未投递：",
	"not delivered: %s":                                "未投递：%s",
	"Created group ":                                   "已创建通信组 ",
	". Press i to review invitations.":                 "。按 i 查看邀请。",
	"Created group %s. Press i to review invitations.": "已创建通信组 %s。按 i 查看邀请。",
	"Private credentials written (share each invitation only with its member):":       "私有凭据已写入（请将邀请交给对应成员）：",
	"Use CLI invite output for the full join instructions; no notification was sent.": "使用 CLI 的 invite 输出获取完整接入说明；本次未发送通知。",
	"group name is required":                 "请输入通信组名称",
	"select at least one session with Space": "请先按空格选择至少一个会话",
	"select a group first":                   "请先选择通信组",
	"Discovering local sessions…":            "正在探测本机会话…",
	"Yes":                                    "是",
	"No":                                     "否",
	"Provider":                               "提供方",
	"Activity":                               "活动情况",
	"Status":                                 "状态",
	"Transport":                              "通信方式",
	"Session":                                "会话",
	"Name / directory":                       "名称 / 目录",
	"Language":                               "语言",
	"Chinese":                                "中文",
	"English":                                "英文",
}

var chineseEvents = map[string]string{
	"group.created":        "通信组已创建",
	"member.invited":       "成员已邀请",
	"member.joined":        "成员已接入",
	"member.removed":       "成员已移除",
	"state.updated":        "任务状态已更新",
	"state.stale":          "任务状态已过期",
	"message.request":      "收到资料请求",
	"message.reply":        "收到资料回复",
	"message.message":      "收到消息",
	"message.read":         "消息已读取",
	"message.expired":      "消息已过期",
	"delivery.accepted":    "投递已接受",
	"delivery.sending":     "正在投递",
	"delivery.failed":      "投递失败",
	"delivery.unsupported": "不支持投递",
	"delivery.unknown":     "投递结果未知",
}
