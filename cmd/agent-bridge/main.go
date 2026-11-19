package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
	"github.com/fuzicheng/agent-bridge/local"
	"github.com/fuzicheng/agent-bridge/mcp"
	"github.com/fuzicheng/agent-bridge/tui"
)

type rpcClient interface {
	Call(context.Context, string, any, *bridge.Credentials, any) error
}

type app struct {
	in              io.Reader
	out             io.Writer
	errOut          io.Writer
	client          rpcClient
	stateDir        string
	socket          string
	credentialsPath string
	jsonOutput      bool
	isTerminal      func() bool
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	a := &app{in: os.Stdin, out: os.Stdout, errOut: os.Stderr, isTerminal: func() bool { info, err := os.Stdin.Stat(); return err == nil && info.Mode()&os.ModeCharDevice != 0 }}
	if err := a.run(ctx, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "agent-bridge:", err)
		os.Exit(1)
	}
}

func (a *app) run(ctx context.Context, args []string) error {
	global := flag.NewFlagSet("agent-bridge", flag.ContinueOnError)
	global.SetOutput(a.errOut)
	global.StringVar(&a.stateDir, "state-dir", local.DefaultStateDir(), "broker data directory")
	global.StringVar(&a.socket, "socket", "", "broker Unix socket path")
	global.StringVar(&a.credentialsPath, "credentials", os.Getenv("AGENT_BRIDGE_CREDENTIALS"), "member credentials file")
	global.BoolVar(&a.jsonOutput, "json", false, "emit JSON")
	global.Usage = func() { fmt.Fprint(a.out, usage) }
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	args = global.Args()
	if len(args) == 0 || args[0] == "help" {
		_, err := fmt.Fprint(a.out, usage)
		return err
	}
	if a.socket == "" {
		a.socket = filepath.Join(a.stateDir, "agent-bridge.sock")
	}
	if a.client == nil {
		a.client = local.Client{Socket: a.socket}
	}
	cmd, rest := args[0], args[1:]
	if cmd == "daemon" {
		if len(rest) > 0 {
			return fmt.Errorf("daemon accepts only global flags before the command")
		}
		if a.credentialsPath != "" {
			return fmt.Errorf("daemon requires owner mode; omit --credentials")
		}
		return local.RunDaemonAt(ctx, a.stateDir, a.socket)
	}
	if cmd == "tui" {
		f := flag.NewFlagSet("tui", flag.ContinueOnError)
		f.SetOutput(a.errOut)
		language := f.String("lang", "zh", "interface language: zh (中文) or en (English)")
		if err := f.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if len(f.Args()) > 0 {
			return fmt.Errorf("usage: agent-bridge [global flags] tui [--lang zh|en]")
		}
		parsedLanguage, err := tui.ParseLanguage(*language)
		if err != nil {
			return err
		}
		if a.credentialsPath != "" {
			return fmt.Errorf("tui requires owner mode; omit --credentials")
		}
		return tui.RunWithLanguage(ctx, local.Client{Socket: a.socket}, parsedLanguage)
	}
	credentials, err := a.credentials()
	if err != nil {
		return err
	}
	if cmd == "mcp" {
		if len(rest) > 0 {
			return fmt.Errorf("usage: agent-bridge --credentials FILE mcp")
		}
		if credentials == nil {
			return fmt.Errorf("mcp requires --credentials FILE or AGENT_BRIDGE_CREDENTIALS")
		}
		return mcp.Serve(ctx, a.in, a.out, local.Client{Socket: a.socket}, *credentials)
	}
	if cmd == "group" {
		return a.group(ctx, rest, credentials)
	}
	return a.command(ctx, cmd, rest, credentials)
}

func (a *app) credentials() (*bridge.Credentials, error) {
	if a.credentialsPath == "" {
		return nil, nil
	}
	f, err := os.Open(a.credentialsPath)
	if err != nil {
		return nil, fmt.Errorf("open credentials: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return nil, fmt.Errorf("credentials must be a regular file smaller than 64 KiB")
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("credentials must be private (chmod 600 %s)", a.credentialsPath)
	}
	var c bridge.Credentials
	if err := json.NewDecoder(io.LimitReader(f, 64<<10)).Decode(&c); err != nil {
		return nil, fmt.Errorf("decode credentials: %w", err)
	}
	if c.GroupID == "" || c.SessionID == "" || c.Token == "" {
		return nil, fmt.Errorf("incomplete credentials")
	}
	return &c, nil
}

func (a *app) flags(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(a.errOut)
	f.BoolVar(&a.jsonOutput, "json", a.jsonOutput, "emit JSON")
	return f
}

// parseFlags supports flags after positional arguments, while preserving '--' and
// treating the value following a non-boolean flag as literal text.
func parseFlags(f *flag.FlagSet, args []string) error {
	var options, positionals []string
	for i := 0; i < len(args); i++ {
		x := args[i]
		if x == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if len(x) < 2 || x[0] != '-' {
			positionals = append(positionals, x)
			continue
		}
		name := strings.TrimLeft(x, "-")
		if name == "" {
			positionals = append(positionals, x)
			continue
		}
		if j := strings.IndexByte(name, '='); j >= 0 {
			options = append(options, x)
			continue
		}
		fl := f.Lookup(name)
		if fl == nil {
			return fmt.Errorf("unknown flag %s", x)
		}
		options = append(options, x)
		if bf, ok := fl.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			continue
		}
		if i+1 >= len(args) {
			return fmt.Errorf("flag %s requires a value", x)
		}
		i++
		options = append(options, args[i])
	}
	return f.Parse(append(options, append([]string{"--"}, positionals...)...))
}

func splitList(s string) []string {
	var out []string
	seen := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" && !seen[part] {
			out = append(out, part)
			seen[part] = true
		}
	}
	return out
}

func (a *app) requireOwner(c *bridge.Credentials) error {
	if c != nil {
		return fmt.Errorf("this command requires owner mode; omit --credentials")
	}
	return nil
}

func requireMember(c *bridge.Credentials) error {
	if c == nil {
		return fmt.Errorf("this command requires --credentials FILE or AGENT_BRIDGE_CREDENTIALS")
	}
	return nil
}

func (a *app) call(ctx context.Context, method string, params any, c *bridge.Credentials) error {
	var result any
	if err := a.client.Call(ctx, method, params, c, &result); err != nil {
		return err
	}
	return a.printJSON(result)
}

func (a *app) printJSON(v any) error {
	e := json.NewEncoder(a.out)
	e.SetIndent("", "  ")
	return e.Encode(v)
}

func (a *app) confirm(prompt string, yes bool) error {
	if yes {
		return nil
	}
	if a.isTerminal == nil || !a.isTerminal() {
		return fmt.Errorf("confirmation requires a terminal; review selected sessions and pass --yes")
	}
	fmt.Fprintf(a.errOut, "%s [y/N] ", prompt)
	answer, err := bufio.NewReader(a.in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if strings.ToLower(strings.TrimSpace(answer)) != "y" && strings.ToLower(strings.TrimSpace(answer)) != "yes" {
		return fmt.Errorf("canceled")
	}
	return nil
}

const usage = `agent-bridge — selected local Codex/Claude session collaboration

Usage: agent-bridge [--state-dir DIR] [--socket PATH] [--credentials FILE] COMMAND

Owner commands:
  daemon                                      Run local broker (foreground)
  discover [--json]                            Discover sessions and capabilities
  status [--json]                              Show group/member state
  tui [--lang zh|en]                           Bilingual session picker (default: zh; l switches)
  group create NAME --members ID,ID [--allow-tmux] [--yes]
  group list | members GROUP | remove GROUP SESSION [--yes] | watch GROUP
  invite GROUP SESSION [--deliver] [--yes]      Write private credentials; optionally notify
  send --group GROUP --to SESSION --body TEXT   Send a message as the owner
  start --provider codex|claude --cwd DIR [--name NAME] [--prompt TEXT]
  events --group GROUP [--after ID] [--limit N]

Member commands (require credentials):
  join [--topics topic,topic]                  Complete agent handshake
  peers                                       Group peers and declared knowledge
  ask SESSION --body TEXT [--ttl-seconds N] [--idempotency-key KEY]
  inbox                                       Read requests and responses
  reply REQUEST --body TEXT [--sources path,url]
  request-status REQUEST                      Check queued/read/answered/expired status
  state --status working|blocked|done|idle [--task TEXT] [--summary TEXT] [--topics LIST]
  heartbeat                                   Report connection liveness
  send --to SESSION --body TEXT                Send a group member message
  mcp                                         Serve tools over MCP stdio

Agent commands emit JSON. Use --json for scripting owner commands.
Protocol: agent-bridge/1 (local request/reply protocol; not A2A).
`

func (a *app) command(ctx context.Context, cmd string, args []string, c *bridge.Credentials) error {
	f := a.flags(cmd)
	var groupID, to, body, message, topics, sources, status, task, summary, provider, cwd, name, prompt, key, requestID string
	var deliver, yes bool
	var ttl, limit int
	var after int64
	switch cmd {
	case "discover", "sessions", "status", "peers", "inbox", "heartbeat":
	case "invite":
		f.BoolVar(&deliver, "deliver", false, "notify target via its available transport")
		f.BoolVar(&yes, "yes", false, "confirm invitation delivery")
	case "join":
		f.StringVar(&topics, "topics", "", "knowledge topics, comma-separated")
	case "ask", "send":
		f.StringVar(&to, "to", "", "target session ID")
		f.StringVar(&body, "body", "", "message text")
		f.StringVar(&message, "message", "", "alias for --body")
		f.StringVar(&key, "idempotency-key", "", "idempotency key")
		f.StringVar(&groupID, "group", "", "group ID (owner send)")
		if cmd == "ask" {
			f.IntVar(&ttl, "ttl-seconds", 900, "request TTL in seconds")
		}
	case "reply":
		f.StringVar(&requestID, "request-id", "", "request ID (alternative to positional argument)")
		f.StringVar(&body, "body", "", "response text")
		f.StringVar(&message, "message", "", "alias for --body")
		f.StringVar(&sources, "sources", "", "comma-separated source references")
	case "request-status":
		f.StringVar(&requestID, "request-id", "", "request ID (alternative to positional argument)")
	case "state":
		f.StringVar(&status, "status", "", "agent task status")
		f.StringVar(&task, "task", "", "current task")
		f.StringVar(&summary, "summary", "", "brief progress summary")
		f.StringVar(&topics, "topics", "", "knowledge topics, comma-separated")
	case "events":
		f.StringVar(&groupID, "group", "", "group ID")
		f.Int64Var(&after, "after", 0, "events after sequence ID")
		f.IntVar(&limit, "limit", 100, "maximum events")
	case "start":
		f.StringVar(&provider, "provider", "", "codex or claude")
		f.StringVar(&cwd, "cwd", "", "working directory")
		f.StringVar(&name, "name", "", "display name")
		f.StringVar(&prompt, "prompt", "", "initial task")
	default:
		return fmt.Errorf("unknown command %q; run agent-bridge help", cmd)
	}
	if err := parseFlags(f, args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	pos := f.Args()
	if body != "" && message != "" {
		return fmt.Errorf("use either --body or --message")
	}
	if body == "" {
		body = message
	}
	switch cmd {
	case "discover":
		if err := a.requireOwner(c); err != nil {
			return err
		}
		if len(pos) > 0 {
			return fmt.Errorf("discover takes no positional arguments")
		}
		var result struct {
			Sessions []bridge.Session `json:"sessions"`
			Warnings []string         `json:"warnings"`
		}
		if err := a.client.Call(ctx, "discover", struct{}{}, nil, &result); err != nil {
			return err
		}
		if a.jsonOutput {
			return a.printJSON(result)
		}
		printSessions(a.out, result.Sessions)
		for _, w := range result.Warnings {
			fmt.Fprintln(a.errOut, "warning:", w)
		}
		return nil
	case "sessions":
		if len(pos) > 0 {
			return fmt.Errorf("sessions takes no positional arguments")
		}
		return a.call(ctx, "sessions", struct{}{}, c)
	case "status":
		if len(pos) > 0 {
			return fmt.Errorf("status takes no positional arguments")
		}
		var result local.Snapshot
		if err := a.client.Call(ctx, "status", struct{}{}, c, &result); err != nil {
			return err
		}
		if a.jsonOutput {
			return a.printJSON(result)
		}
		printSessions(a.out, result.Sessions)
		printMembers(a.out, result.Members)
		for _, w := range result.Warnings {
			fmt.Fprintln(a.errOut, "warning:", w)
		}
		return nil
	case "invite":
		if err := a.requireOwner(c); err != nil {
			return err
		}
		if len(pos) != 2 {
			return fmt.Errorf("usage: invite GROUP SESSION [--deliver] [--yes]")
		}
		if deliver {
			if err := a.confirm(fmt.Sprintf("Deliver an invitation to %s in group %s?", pos[1], pos[0]), yes); err != nil {
				return err
			}
		}
		return a.call(ctx, "invite", map[string]any{"group_id": pos[0], "session_id": pos[1], "deliver": deliver}, nil)
	case "join":
		if err := requireMember(c); err != nil {
			return err
		}
		if len(pos) > 0 {
			return fmt.Errorf("join takes no positional arguments")
		}
		return a.call(ctx, "join", map[string]any{"topics": splitList(topics)}, c)
	case "peers", "inbox", "heartbeat":
		if err := requireMember(c); err != nil {
			return err
		}
		if len(pos) > 0 {
			return fmt.Errorf("%s takes no positional arguments", cmd)
		}
		return a.call(ctx, cmd, struct{}{}, c)
	case "ask", "send":
		if len(pos) == 1 && to == "" {
			to = pos[0]
		} else if len(pos) > 0 {
			return fmt.Errorf("usage: %s [SESSION] --body TEXT", cmd)
		}
		if to == "" || body == "" {
			return fmt.Errorf("%s requires a target and --body TEXT", cmd)
		}
		if cmd == "ask" {
			if err := requireMember(c); err != nil {
				return err
			}
		}
		if c != nil {
			groupID = c.GroupID
		} else if groupID == "" {
			return fmt.Errorf("owner send requires --group GROUP")
		}
		return a.call(ctx, cmd, map[string]any{"group_id": groupID, "to": to, "body": body, "ttl_seconds": ttl, "idempotency_key": key}, c)
	case "reply":
		if err := requireMember(c); err != nil {
			return err
		}
		if len(pos) == 1 && requestID == "" {
			requestID = pos[0]
		} else if len(pos) > 0 {
			return fmt.Errorf("provide one request ID")
		}
		if requestID == "" || body == "" {
			return fmt.Errorf("usage: reply REQUEST --body TEXT [--sources LIST]")
		}
		return a.call(ctx, "reply", map[string]any{"request_id": requestID, "body": body, "sources": splitList(sources)}, c)
	case "request-status":
		if err := requireMember(c); err != nil {
			return err
		}
		if len(pos) == 1 && requestID == "" {
			requestID = pos[0]
		} else if len(pos) > 0 {
			return fmt.Errorf("provide one request ID")
		}
		if requestID == "" {
			return fmt.Errorf("usage: request-status REQUEST")
		}
		return a.call(ctx, "request.status", map[string]string{"request_id": requestID}, c)
	case "state":
		if err := requireMember(c); err != nil {
			return err
		}
		if len(pos) > 0 || status == "" || !bridge.Status(status).Valid() {
			return fmt.Errorf("state requires --status idle|working|blocked|done|stale|unknown")
		}
		return a.call(ctx, "state", map[string]any{"status": status, "task": task, "summary": summary, "topics": splitList(topics)}, c)
	case "events":
		if len(pos) > 0 {
			return fmt.Errorf("events takes no positional arguments")
		}
		if c != nil {
			groupID = c.GroupID
		}
		if groupID == "" {
			return fmt.Errorf("events requires --group GROUP")
		}
		if limit <= 0 || limit > 1000 || after < 0 {
			return fmt.Errorf("events requires limit 1..1000 and nonnegative after")
		}
		return a.call(ctx, "events", map[string]any{"group_id": groupID, "after": after, "limit": limit}, c)
	case "start":
		if err := a.requireOwner(c); err != nil {
			return err
		}
		if len(pos) > 0 || (provider != "codex" && provider != "claude") || cwd == "" {
			return fmt.Errorf("usage: start --provider codex|claude --cwd DIR [--prompt TEXT]")
		}
		abs, err := filepath.Abs(cwd)
		if err != nil {
			return err
		}
		return a.call(ctx, "managed.start", map[string]string{"provider": provider, "cwd": abs, "name": name, "prompt": prompt}, nil)
	}
	return fmt.Errorf("unhandled command %s", cmd)
}

func (a *app) group(ctx context.Context, args []string, c *bridge.Credentials) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: group create|list|members|remove|watch")
	}
	sub := args[0]
	f := a.flags("group " + sub)
	var members string
	var allowTmux, yes bool
	if sub == "create" {
		f.StringVar(&members, "members", "", "comma-separated session IDs")
		f.BoolVar(&allowTmux, "allow-tmux", false, "allow tmux delivery for selected members")
		f.BoolVar(&yes, "yes", false, "confirm creation")
	}
	if sub == "remove" {
		f.BoolVar(&yes, "yes", false, "confirm removing member")
	}
	if err := parseFlags(f, args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	pos := f.Args()
	switch sub {
	case "create":
		if err := a.requireOwner(c); err != nil {
			return err
		}
		ids := splitList(members)
		if len(pos) != 1 || len(ids) == 0 {
			return fmt.Errorf("usage: group create NAME --members ID,ID [--allow-tmux] [--yes]")
		}
		prompt := fmt.Sprintf("Create group %q with %s (tmux delivery allowed: %t)?", pos[0], strings.Join(ids, ", "), allowTmux)
		if err := a.confirm(prompt, yes); err != nil {
			return err
		}
		return a.call(ctx, "group.create", map[string]any{"name": pos[0], "members": ids, "allow_tmux": allowTmux}, nil)
	case "list":
		if len(pos) > 0 {
			return fmt.Errorf("usage: group list")
		}
		return a.call(ctx, "group.list", struct{}{}, c)
	case "members":
		if len(pos) != 1 {
			return fmt.Errorf("usage: group members GROUP")
		}
		return a.call(ctx, "group.members", map[string]string{"group_id": pos[0]}, c)
	case "remove":
		if err := a.requireOwner(c); err != nil {
			return err
		}
		if len(pos) != 2 {
			return fmt.Errorf("usage: group remove GROUP SESSION [--yes]")
		}
		if err := a.confirm(fmt.Sprintf("Remove %s from %s and revoke its credentials?", pos[1], pos[0]), yes); err != nil {
			return err
		}
		return a.call(ctx, "group.remove", map[string]string{"group_id": pos[0], "session_id": pos[1]}, nil)
	case "watch":
		if len(pos) != 1 {
			return fmt.Errorf("usage: group watch GROUP")
		}
		return a.watch(ctx, pos[0], c)
	default:
		return fmt.Errorf("unknown group command %q", sub)
	}
}

func (a *app) watch(ctx context.Context, groupID string, c *bridge.Credentials) error {
	timer := time.NewTicker(2 * time.Second)
	defer timer.Stop()
	for {
		var members []bridge.Member
		if err := a.client.Call(ctx, "group.members", map[string]string{"group_id": groupID}, c, &members); err != nil {
			return err
		}
		if a.jsonOutput {
			if err := a.printJSON(members); err != nil {
				return err
			}
		} else {
			fmt.Fprintln(a.out, time.Now().Format(time.RFC3339))
			printMembers(a.out, members)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func printSessions(w io.Writer, sessions []bridge.Session) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "SESSION\tPROVIDER\tACTIVE\tTRANSPORT\tSTATUS\tNAME / DIRECTORY")
	for _, s := range sessions {
		fmt.Fprintf(tw, "%s\t%s\t%t\t%s\t%s\t%s %s\n", s.ID, s.Provider, s.Active, s.Capabilities.Transport, s.Status, s.Name, s.CWD)
	}
	_ = tw.Flush()
}

func printMembers(w io.Writer, members []bridge.Member) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "GROUP\tSESSION\tJOINED\tTASK STATUS\tTASK / SUMMARY")
	for _, m := range members {
		fmt.Fprintf(tw, "%s\t%s\t%t\t%s\t%s %s\n", m.GroupID, m.Session.ID, m.Joined, m.State.Status, m.State.Task, m.State.Summary)
	}
	_ = tw.Flush()
}
