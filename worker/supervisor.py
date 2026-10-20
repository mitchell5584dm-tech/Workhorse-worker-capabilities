"""Bounded process supervisor; SQLite queue shared on one local filesystem."""
import multiprocessing
import signal
import time
from pathlib import Path
from workhorse import connect, claim, execute, finish
import json


def perform(database, root, job, policy):
    db = connect(database)
    try:
        result = execute(job['capability'], json.loads(job['payload']), root, policy)
        finish(db, job, result=result)
    except Exception as exc:
        finish(db, job, error=f'{type(exc).__name__}: {exc}', retryable=isinstance(exc, OSError))
    finally:
        db.close()


def serve(database, root, concurrency=8, idle_exit=0, task_timeout=240, policy=None):
    if type(concurrency) is not int or not 1 <= concurrency <= 32:
        raise ValueError('concurrency must be 1..32')
    if idle_exit < 0 or not 0 < task_timeout < 290:
        raise ValueError('Invalid idle/timeout configuration')
    root = str(Path(root).resolve(strict=True))
    database = str(Path(database).resolve())
    db = connect(database)
    context = multiprocessing.get_context('spawn')
    active = {}
    stopping = False
    completed = 0
    idle_since = time.monotonic()

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    old_handlers = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        while True:
            for job_id, (process, started, job) in list(active.items()):
                timeout = time.monotonic() - started >= task_timeout
                if timeout and process.is_alive():
                    process.terminate()
                    process.join(2)
                    if process.is_alive():
                        process.kill()
                if not process.is_alive() or timeout:
                    process.join()
                    if timeout or process.exitcode != 0:
                        finish(db, job, error='Execution timeout' if timeout else 'Worker process crashed', retryable=True)
                    process.close()
                    del active[job_id]
                    completed += 1
                    idle_since = time.monotonic()
            if not stopping:
                while len(active) < concurrency:
                    job = claim(db, max_running=concurrency)
                    if job is None:
                        break
                    process = context.Process(target=perform, args=(database, root, job, policy))
                    try:
                        process.start()
                    except Exception:
                        finish(db, job, error='Unable to start worker process', retryable=True)
                        raise
                    active[job['id']] = (process, time.monotonic(), job)
                    idle_since = time.monotonic()
            if stopping and not active:
                break
            if not active and idle_exit and time.monotonic() - idle_since >= idle_exit:
                break
            time.sleep(0.05)
    finally:
        for process, _, job in active.values():
            if process.is_alive():
                process.terminate()
                process.join(2)
                if process.is_alive():
                    process.kill()
                    process.join()
            finish(db, job, error='Supervisor stopped', retryable=True)
            process.close()
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        db.close()
    return {'processed_attempts': completed, 'concurrency_limit': concurrency}
