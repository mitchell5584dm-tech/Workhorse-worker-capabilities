#!/usr/bin/env python3
"""Workhorse: durable, local, allowlisted task execution. Python 3.11+."""
import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
import uuid
from pathlib import Path

CAPABILITIES = ('inventory', 'validate_json', 'unit_economics', 'capacity_plan', 'replenishment_plan', 'engineering_plan', 'command')


def connect(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL, capability TEXT NOT NULL,
      payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued', attempts INTEGER DEFAULT 0,
      max_attempts INTEGER NOT NULL, available REAL DEFAULT 0, lease_until REAL,
      token TEXT, result TEXT, error TEXT, created REAL NOT NULL,
      priority INTEGER NOT NULL DEFAULT 0, resource TEXT, dependencies TEXT NOT NULL DEFAULT '[]');
    CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state, available, priority, created);
    CREATE TABLE IF NOT EXISTS events (
      sequence INTEGER PRIMARY KEY, job_id TEXT, at REAL, event TEXT, detail TEXT);
    ''')
    return db


def event(db, job_id, name, detail=''):
    db.execute('INSERT INTO events(job_id,at,event,detail) VALUES(?,?,?,?)',
               (job_id, time.time(), name, detail))


def submit(db, capability, payload, request_key, max_attempts=3, priority=0, resource=None, dependencies=None, max_pending=10000):
    if capability not in CAPABILITIES:
        raise ValueError('Unknown capability')
    if not isinstance(payload, dict) or not 1 <= max_attempts <= 5:
        raise ValueError('Payload must be an object; attempts must be 1..5')
    if capability == 'command' and resource is None:
        resource = 'command:workspace'
    dependencies = dependencies or []
    if type(priority) is not int or not -100 <= priority <= 100:
        raise ValueError('priority must be -100..100')
    if not isinstance(request_key, str) or not 1 <= len(request_key) <= 200:
        raise ValueError('Request key must contain 1..200 characters')
    if resource is not None and (not isinstance(resource, str) or not 1 <= len(resource) <= 200):
        raise ValueError('Resource must contain 1..200 characters')
    if not isinstance(dependencies, list) or any(not isinstance(d, str) for d in dependencies) or len(dependencies) > 100:
        raise ValueError('dependencies must contain at most 100 job IDs')
    deps = json.dumps(sorted(set(dependencies)))
    encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
    if len(encoded) > 65536:
        raise ValueError('Payload exceeds 64 KiB')
    with db:
        db.execute('BEGIN IMMEDIATE')
        existing = db.execute('SELECT * FROM jobs WHERE request_key=?', (request_key,)).fetchone()
        if existing:
            if (existing['capability'], existing['payload'], existing['max_attempts'], existing['priority'], existing['resource'], existing['dependencies']) != (capability, encoded, max_attempts, priority, resource, deps):
                raise ValueError('Request key already belongs to a different task')
            return existing['id']
        pending = db.execute("SELECT count(*) FROM jobs WHERE state IN ('queued','running')").fetchone()[0]
        if pending >= max_pending:
            raise ValueError('Queue capacity reached; apply backpressure and retry later')
        for dependency in dependencies:
            if not db.execute('SELECT 1 FROM jobs WHERE id=?', (dependency,)).fetchone():
                raise ValueError('Dependency must already exist: ' + dependency)
        job_id = str(uuid.uuid4())
        db.execute('INSERT INTO jobs(id,request_key,capability,payload,max_attempts,created,priority,resource,dependencies) VALUES(?,?,?,?,?,?,?,?,?)',
                   (job_id, request_key, capability, encoded, max_attempts, time.time(), priority, resource, deps))
        event(db, job_id, 'submitted')
    return job_id


def claim(db, lease_seconds=300, max_running=8):
    now = time.time()
    db.execute('BEGIN IMMEDIATE')
    try:
        expired = db.execute("SELECT * FROM jobs WHERE state='running' AND lease_until<=?", (now,)).fetchall()
        for job in expired:
            state = 'failed' if job['attempts'] >= job['max_attempts'] else 'queued'
            db.execute('UPDATE jobs SET state=?,token=NULL,lease_until=NULL,error=? WHERE id=?',
                       (state, 'Worker lease expired', job['id']))
            event(db, job['id'], 'lease_expired', state)
        # Propagate dependency failures transitively before selecting runnable work.
        while True:
            blocked = db.execute("""SELECT DISTINCT j.id FROM jobs j, json_each(j.dependencies) d
                JOIN jobs upstream ON upstream.id=d.value
                WHERE j.state='queued' AND upstream.state IN ('failed','cancelled','blocked')""").fetchall()
            if not blocked:
                break
            for row in blocked:
                db.execute("UPDATE jobs SET state='blocked',error='Dependency did not succeed' WHERE id=?", (row['id'],))
                event(db, row['id'], 'blocked')
        active = db.execute("SELECT count(*) FROM jobs WHERE state='running'").fetchone()[0]
        if active >= max_running:
            db.commit()
            return None
        job = db.execute("""SELECT * FROM jobs j WHERE state='queued' AND available<=?
            AND NOT EXISTS (SELECT 1 FROM json_each(j.dependencies) d
                JOIN jobs upstream ON upstream.id=d.value WHERE upstream.state != 'succeeded')
            AND (resource IS NULL OR NOT EXISTS
                (SELECT 1 FROM jobs running WHERE running.state='running' AND running.resource=j.resource))
            ORDER BY priority DESC,created,id LIMIT 1""", (now,)).fetchone()
        if not job:
            db.commit()
            return None
        token = str(uuid.uuid4())
        db.execute("UPDATE jobs SET state='running',attempts=attempts+1,token=?,lease_until=? WHERE id=?",
                   (token, now + lease_seconds, job['id']))
        event(db, job['id'], 'claimed')
        db.commit()
        return dict(db.execute('SELECT * FROM jobs WHERE id=?', (job['id'],)).fetchone())
    except BaseException:
        db.rollback()
        raise


def finish(db, job, result=None, error=None, retryable=False):
    with db:
        db.execute('BEGIN IMMEDIATE')
        current = db.execute('SELECT * FROM jobs WHERE id=?', (job['id'],)).fetchone()
        if current['state'] != 'running' or current['token'] != job['token'] or current['lease_until'] <= time.time():
            return False  # A cancelled or expired worker cannot overwrite newer results.
        state = 'succeeded' if error is None else 'failed'
        if error and retryable and current['attempts'] < current['max_attempts']:
            state = 'queued'
        db.execute('UPDATE jobs SET state=?,result=?,error=?,available=?,token=NULL,lease_until=NULL WHERE id=?',
                   (state, json.dumps(result, allow_nan=False) if error is None else None,
                    error, time.time() + min(60, 2 ** current['attempts']), job['id']))
        event(db, job['id'], state)
    return True


def confined(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError('Path escapes workspace')
    if '.git' in path.relative_to(root).parts:
        raise ValueError('Git internals are excluded')
    return path


def execute(capability, payload, root, policy=None):
    root = Path(root).resolve(strict=True)
    if capability == 'command':
        from commands import run_command
        return run_command(payload, root, policy)
    if capability in ('capacity_plan', 'replenishment_plan', 'engineering_plan'):
        from planning import calculate
        return calculate(capability, payload)
    if capability == 'unit_economics':
        def number(key, default=None):
            value = payload.get(key, default)
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
                raise ValueError(f'{key} must be a finite nonnegative number')
            return value
        price, cost = number('price'), number('unit_cost')
        fee, fixed = number('fee_rate', 0), number('fixed_fee', 0)
        target = number('target_margin', 0.3)
        if fee + target >= 1:
            raise ValueError('fee_rate + target_margin must be below 1')
        net = price * (1 - fee) - cost - fixed
        return {'contribution': round(net, 4), 'margin': net / price if price else None,
                'minimum_price_for_target': math.ceil((cost + fixed) / (1 - fee - target) * 100) / 100,
                'advisory_only': True, 'currency': payload.get('currency', 'USD')}
    if capability == 'validate_json':
        path = confined(root, payload['path'])
        if path.stat().st_size > 1024 * 1024:
            raise ValueError('JSON input exceeds 1 MiB')
        raw = path.read_bytes()
        def reject(value):
            raise ValueError('Nonfinite JSON constant: ' + value)
        value = json.loads(raw, parse_constant=reject)
        return {'valid': True, 'type': type(value).__name__, 'sha256': hashlib.sha256(raw).hexdigest()}
    if capability == 'inventory':
        path = confined(root, payload.get('path', '.'))
        limit = payload.get('limit', 1000)
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise ValueError('limit must be 1..10000')
        # Never follow symlinks or inspect hidden directories/files.
        import os
        records = []
        if not path.is_dir():
            raise ValueError('Inventory path must be a directory')
        for directory, dirs, files in os.walk(path, followlinks=False):
            dirs[:] = sorted(d for d in dirs if not d.startswith('.') and d not in ('node_modules', '__pycache__') and not (Path(directory) / d).is_symlink())
            for name in sorted(files):
                candidate = Path(directory) / name
                if name.startswith('.') or candidate.is_symlink():
                    continue
                if len(records) >= limit:
                    return {'files': records, 'truncated': True}
                records.append({'path': str(candidate.relative_to(root)), 'bytes': candidate.stat().st_size})
        return {'files': records, 'truncated': False}
    raise ValueError('Unknown capability')


def run_one(db, root, max_running=8, policy=None):
    job = claim(db, max_running=max_running)
    if job is None:
        return None
    try:
        result = execute(job['capability'], json.loads(job['payload']), root, policy)
        finish(db, job, result=result)
    except Exception as exc:
        finish(db, job, error=f'{type(exc).__name__}: {exc}', retryable=isinstance(exc, OSError))
    return job['id']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='workhorse.sqlite3')
    parser.add_argument('--policy', help='Operator-controlled JSON command allowlist outside the task workspace')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('capabilities')
    sub.add_parser('metrics')
    serve = sub.add_parser('serve')
    serve.add_argument('--root', required=True)
    serve.add_argument('--concurrency', type=int, default=8)
    serve.add_argument('--idle-exit', type=float, default=0, help='Exit after this many idle seconds; zero runs continuously')
    add = sub.add_parser('submit')
    add.add_argument('capability', choices=CAPABILITIES)
    add.add_argument('--payload', required=True, help='JSON object')
    add.add_argument('--key', required=True)
    add.add_argument('--attempts', type=int, default=3)
    add.add_argument('--priority', type=int, default=0)
    add.add_argument('--resource')
    add.add_argument('--depends-on', action='append', default=[])
    run = sub.add_parser('run')
    run.add_argument('--root', required=True)
    for name in ('status', 'events', 'cancel'):
        sub.add_parser(name).add_argument('id')
    args = parser.parse_args()
    # Allow the DB path to be overridden by an environment variable so the
    # start command in Render's dashboard doesn't need to be changed manually.
    args.db = os.environ.get('WORKHORSE_DB', args.db)
    if args.command == 'capabilities':
        print(json.dumps(CAPABILITIES))
        return
    if args.command == 'serve':
        from supervisor import serve
        print(json.dumps(serve(args.db, args.root, args.concurrency, args.idle_exit, policy=args.policy)))
        return
    db = connect(args.db)
    if args.command == 'metrics':
        output = {row['state']: row['n'] for row in db.execute('SELECT state,count(*) AS n FROM jobs GROUP BY state')}
    elif args.command == 'submit':
        output = {'id': submit(db, args.capability, json.loads(args.payload), args.key, args.attempts, args.priority, args.resource, args.depends_on)}
    elif args.command == 'run':
        Path(args.root).resolve(strict=True)
        output = {'processed': run_one(db, args.root, policy=args.policy)}
    elif args.command == 'events':
        output = [dict(row) for row in db.execute('SELECT * FROM events WHERE job_id=? ORDER BY sequence', (args.id,))]
    elif args.command == 'cancel':
        with db:
            changed = db.execute("UPDATE jobs SET state='cancelled',token=NULL,lease_until=NULL WHERE id=? AND state IN ('queued','running')", (args.id,)).rowcount
            if changed:
                event(db, args.id, 'cancelled')
        output = {'cancelled': bool(changed)}
    else:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (args.id,)).fetchone()
        output = dict(row) if row else None
    print(json.dumps(output, indent=2))
    db.close()


if __name__ == '__main__':
    main()
