"""Operator-defined commands, never shell text or task-provided executable paths."""
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile


def run_command(payload, root, policy):
    if policy is None:
        raise ValueError('Command execution requires an operator policy')
    policy_path = Path(policy).resolve(strict=True)
    if policy_path.is_relative_to(root):
        raise ValueError('Keep the operator policy outside the task workspace')
    if policy_path.stat().st_size > 65536:
        raise ValueError('Policy exceeds 64 KiB')
    config = json.loads(policy_path.read_text())
    action = payload.get('action')
    if set(payload) != {'action'} or not isinstance(action, str):
        raise ValueError('Command payload accepts only an action name')
    definition = config.get('actions', {}).get(action)
    if not isinstance(definition, dict):
        raise ValueError('Action is not allowlisted')
    argv = definition.get('argv')
    if not isinstance(argv, list) or not argv or any(not isinstance(v, str) for v in argv):
        raise ValueError('Action must define an argv list')
    if not Path(argv[0]).is_absolute():
        raise ValueError('Executable path must be absolute')
    timeout = definition.get('timeout_seconds', 120)
    if type(timeout) not in (float, int) or not 0 < timeout <= 180:
        raise ValueError('Command timeout must be in (0,180] seconds')
    # Do not inherit API keys, tokens, cloud credentials, or arbitrary environment settings.
    env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8', 'PYTHONDONTWRITEBYTECODE': '1'}
    if os.name != 'posix':
        raise ValueError('Command isolation currently requires a POSIX host')
    import resource
    def limits():
        resource.setrlimit(resource.RLIMIT_FSIZE, (2 * 1024 * 1024, 2 * 1024 * 1024))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(argv, cwd=root, env=env, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                   preexec_fn=limits)
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise TimeoutError('Allowlisted command exceeded its timeout')
        finally:
            # Remove descendants even if the direct command has already exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        size = log.tell()
        log.seek(0)
        output = log.read(32768).decode('utf-8', errors='replace')
        if returncode:
            raise ValueError(f'Action {action} exited with status {returncode}: {output}')
        return {'action': action, 'exit_code': returncode, 'output': output,
                'output_truncated': size > 32768}
