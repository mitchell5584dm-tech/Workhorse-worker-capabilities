"""Reproducible process-runner smoke benchmark; not an application throughput claim."""
import argparse
import json
from pathlib import Path
import tempfile
import time
from workhorse import connect, submit
from supervisor import serve


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=200)
    parser.add_argument('--workers', type=int, default=8)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as directory:
        db_path = str(Path(directory) / 'queue.sqlite3')
        root = Path(directory) / 'workspace'
        root.mkdir()
        db = connect(db_path)
        for i in range(args.jobs):
            submit(db, 'capacity_plan', {'jobs_per_hour': 1000, 'seconds_per_job': 30,
                   'memory_mb_per_worker': 128, 'memory_budget_mb': 4096}, str(i))
        start = time.monotonic()
        run = serve(db_path, root, args.workers, idle_exit=0.2)
        elapsed = time.monotonic() - start
        states = {r['state']: r['n'] for r in db.execute('SELECT state,count(*) n FROM jobs GROUP BY state')}
        attempts = db.execute('SELECT sum(attempts) FROM jobs').fetchone()[0]
        print(json.dumps({'jobs': args.jobs, 'workers': args.workers, 'elapsed_seconds': round(elapsed, 3),
                          'jobs_per_second': round(args.jobs / elapsed, 2), 'states': states,
                          'total_attempts': attempts, 'runner': run,
                          'workload': 'small deterministic capacity calculations; includes process startup and idle-exit overhead'}, indent=2))
        db.close()
        if states != {'succeeded': args.jobs} or attempts != args.jobs:
            raise SystemExit('Benchmark correctness check failed')


if __name__ == '__main__':
    main()
