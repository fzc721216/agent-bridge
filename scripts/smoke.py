#!/usr/bin/env python3
"""Exercise real CLI/daemon/SQLite/provider pipes using simulated agents only.

No model endpoint, installed provider credentials or existing session is used.
Build bin/agent-bridge first, then run: python3 scripts/smoke.py
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


FAKE_AGENT = r'''#!/usr/bin/env python3
import json, os, shlex, subprocess, sys

codex = 'app-server' in sys.argv
sid = '00000000-0000-4000-8000-000000000001'
if not codex:
    sid = sys.argv[sys.argv.index('--session-id') + 1]
base = None
turn = 0

def emit(value):
    print(json.dumps(value), flush=True)

def run_cli(args):
    result = subprocess.run(base + args, capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return json.loads(result.stdout)

def handle(text):
    global base
    for line in text.splitlines():
        if line.startswith('First complete the handshake: '):
            argv = shlex.split(line.split(': ', 1)[1])
            if argv[0] != os.environ['AB_SMOKE_BINARY']:
                raise RuntimeError('unexpected executable in invitation')
            base = argv[:argv.index('join')]
            run_cli(['join', '--topics', 'order schema' if not codex else 'API implementation'])
            run_cli(['state', '--status', 'working', '--task', 'Inspect order status'])
            return
    if base and '[agent-bridge]' in text:
        for message in run_cli(['inbox']):
            if message['type'] == 'request' and message['state'] != 'answered':
                run_cli(['reply', '--request-id', message['message_id'],
                         '--body', 'status=1 means paid', '--sources', 'schema.sql:12'])

if not codex:
    emit({'type':'system', 'subtype':'init', 'session_id':sid})
for line in sys.stdin:
    message = json.loads(line)
    if codex:
        method, ident = message.get('method'), message.get('id')
        if method == 'initialize':
            emit({'id':ident, 'result':{'userAgent':'simulated-agent'}})
        elif method == 'thread/start':
            emit({'id':ident, 'result':{'thread':{'id':sid}}})
        elif method == 'turn/start':
            turn += 1
            tid = 'turn-' + str(turn)
            emit({'id':ident, 'result':{'turn':{'id':tid}}})
            handle(message['params']['input'][0]['text'])
            emit({'method':'turn/completed', 'params':{'threadId':sid, 'turn':{'id':tid,'status':'completed'}}})
    elif message.get('type') == 'user':
        handle(message['message']['content'])
        emit({'type':'result', 'subtype':'success', 'is_error':False, 'session_id':sid})
'''


def main():
    binary = str(Path(sys.argv[1] if len(sys.argv) > 1 else 'bin/agent-bridge').resolve())
    if not Path(binary).is_file():
        raise RuntimeError('build the executable first: go build -o bin/agent-bridge ./cmd/agent-bridge')
    with tempfile.TemporaryDirectory(prefix='ab-e2e-') as temporary:
        root = Path(temporary)
        tools = root / 'tools'
        tools.mkdir()
        for name in ('codex', 'claude'):
            executable = tools / name
            executable.write_text(FAKE_AGENT)
            executable.chmod(0o700)
        repo = root / 'repo'
        repo.mkdir()
        env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ.get('PATH', ''),
                   CODEX_HOME=str(root / 'codex-home'), CLAUDE_CONFIG_DIR=str(root / 'claude-home'),
                   AB_SMOKE_BINARY=binary, AGENT_BRIDGE_CREDENTIALS='')
        state = root / 'state'
        prefix = [binary, '--state-dir', str(state)]

        def cli(*args, credentials=None):
            cmd = prefix + (['--credentials', credentials] if credentials else []) + list(args)
            result = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=25)
            if result.returncode:
                raise RuntimeError(f'{args[0]} failed: {result.stderr}')
            return json.loads(result.stdout)

        def await_value(check, description, seconds=12):
            deadline = time.monotonic() + seconds
            last_error = None
            while time.monotonic() < deadline:
                try:
                    value = check()
                    if value:
                        return value
                except (RuntimeError, OSError) as error:
                    last_error = error
                time.sleep(0.05)
            raise RuntimeError(f'timed out waiting for {description}: {last_error}')

        def start_daemon():
            process = subprocess.Popen(prefix + ['daemon'], env=env, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.PIPE, text=True)
            try:
                await_value(lambda: cli('status', '--json'), 'daemon startup')
            except Exception:
                stop_daemon(process)
                raise
            return process

        def stop_daemon(process):
            process.send_signal(signal.SIGTERM)
            try:
                _, error = process.communicate(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise RuntimeError('daemon failed to stop')
            if process.returncode:
                raise RuntimeError(f'daemon exited with {process.returncode}: {error}')

        process = start_daemon()
        try:
            a = cli('start', '--provider', 'codex', '--cwd', str(repo), '--name', 'API')
            b = cli('start', '--provider', 'claude', '--cwd', str(repo), '--name', 'Schema')
            group = cli('group', 'create', 'knowledge', '--members', a['id'] + ',' + b['id'], '--yes')
            invites = [cli('invite', group['id'], s['id'], '--deliver', '--yes') for s in (a, b)]
            assert all(i['delivered'] for i in invites), 'provider notification not accepted'

            def joined():
                members = cli('group', 'members', group['id'])
                return members if len(members) == 2 and all(m['joined'] for m in members) else None

            await_value(joined, 'both simulated agents completing join')
            creds_a, creds_b = [i['credentials_path'] for i in invites]
            request = cli('ask', '--to', b['id'], '--body', 'What does order.status=1 mean?',
                          '--idempotency-key', 'schema-question', credentials=creds_a)

            def answered():
                result = cli('request-status', request['message_id'], credentials=creds_a)
                return result if result['state'] == 'answered' else None

            await_value(answered, 'request notification and automatic simulated reply')
            answers = cli('inbox', credentials=creds_a)
            assert len(answers) == 1 and answers[0]['body'] == 'status=1 means paid'
            assert answers[0]['sources'] == ['schema.sql:12']
            assert answers[0]['delivery'] == 'inbox', 'a reply must not create a wakeup loop'
            assert len(cli('peers', credentials=creds_b)) == 2
            print('PASS: managed Codex/Claude -> selected group -> invitations -> real join -> ask -> notify -> reply with source')
        finally:
            stop_daemon(process)

        process = start_daemon()
        try:
            assert len(cli('group', 'list')) == 1
            assert cli('request-status', request['message_id'], credentials=creds_a)['state'] == 'answered'
            assert cli('inbox', credentials=creds_a)[0]['sources'] == ['schema.sql:12']
            print('PASS: daemon restart retains group, scoped credentials, answered request and reply')
        finally:
            stop_daemon(process)


if __name__ == '__main__':
    main()
