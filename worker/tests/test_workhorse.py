import concurrent.futures
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import workhorse as w
from planning import calculate
from supervisor import serve


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / 'workspace'
        self.root.mkdir()
        self.database = str(Path(self.temp.name) / 'queue.sqlite3')
        self.db = w.connect(self.database)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def add(self, key='a', **kwargs):
        return w.submit(self.db, 'inventory', {}, key, **kwargs)

    def state(self, job):
        return self.db.execute('SELECT state FROM jobs WHERE id=?', (job,)).fetchone()[0]

    def test_idempotent_and_conflicting_submission(self):
        self.assertEqual(self.add(), self.add())
        with self.assertRaises(ValueError):
            self.add(priority=1)

    def test_backpressure(self):
        self.add()
        with self.assertRaises(ValueError):
            self.add('b', max_pending=1)

    def test_unknown_dependency(self):
        with self.assertRaises(ValueError):
            self.add(dependencies=['missing'])

    def test_priority(self):
        self.add()
        high = self.add('high', priority=10)
        self.assertEqual(w.claim(self.db)['id'], high)

    def test_dependencies(self):
        parent = self.add()
        child = self.add('b', priority=100, dependencies=[parent])
        job = w.claim(self.db)
        self.assertEqual(job['id'], parent)
        self.assertIsNone(w.claim(self.db))
        w.finish(self.db, job, result={})
        self.assertEqual(w.claim(self.db)['id'], child)

    def test_failure_propagates(self):
        parent = self.add()
        child = self.add('b', dependencies=[parent])
        grandchild = self.add('c', dependencies=[child])
        w.finish(self.db, w.claim(self.db), error='invalid')
        self.assertIsNone(w.claim(self.db))
        self.assertEqual(self.state(grandchild), 'blocked')

    def test_resource_lock(self):
        self.add(resource='repo')
        self.add('b', resource='repo')
        self.add('c', resource='other')
        first = w.claim(self.db)
        second = w.claim(self.db)
        self.assertNotEqual(first['resource'], second['resource'])
        self.assertIsNone(w.claim(self.db))

    def test_concurrency_limit(self):
        self.add()
        self.add('b')
        w.claim(self.db, max_running=1)
        self.assertIsNone(w.claim(self.db, max_running=1))

    def test_expired_lease_reclaims_and_rejects_stale_result(self):
        job_id = self.add()
        stale = w.claim(self.db, lease_seconds=-1)
        fresh = w.claim(self.db)
        self.assertEqual(fresh['id'], job_id)
        self.assertFalse(w.finish(self.db, stale, result={'stale': True}))
        self.assertTrue(w.finish(self.db, fresh, result={}))

    def test_retry_budget(self):
        job_id = self.add(max_attempts=1)
        stale = w.claim(self.db, lease_seconds=-1)
        self.assertIsNone(w.claim(self.db))
        self.assertEqual(self.state(job_id), 'failed')
        self.assertFalse(w.finish(self.db, stale, result={}))

    def test_backoff(self):
        self.add()
        w.finish(self.db, w.claim(self.db), error='temporary', retryable=True)
        self.assertIsNone(w.claim(self.db))

    def test_cancel_fences_result(self):
        job_id = self.add()
        job = w.claim(self.db)
        with self.db:
            self.db.execute("UPDATE jobs SET state='cancelled',token=NULL WHERE id=?", (job_id,))
        self.assertFalse(w.finish(self.db, job, result={}))

    def test_concurrent_claims_unique(self):
        for n in range(100):
            self.add(str(n))
        def consume(_):
            db = w.connect(self.database)
            ids = []
            while job := w.claim(db):
                ids.append(job['id'])
                w.finish(db, job, result={})
            db.close()
            return ids
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            ids = sum(pool.map(consume, range(8)), [])
        self.assertEqual(len(ids), 100)
        self.assertEqual(len(set(ids)), 100)

    def test_path_escape_and_symlink(self):
        with self.assertRaises(ValueError):
            w.execute('inventory', {'path': '..'}, self.root)
        (self.root / 'escape').symlink_to(self.root.parent, target_is_directory=True)
        with self.assertRaises(ValueError):
            w.execute('inventory', {'path': 'escape'}, self.root)

    def test_json_evidence(self):
        (self.root / 'data.json').write_text('{"ok":true}')
        result = w.execute('validate_json', {'path': 'data.json'}, self.root)
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['sha256']), 64)

    def test_inventory_bound(self):
        for i in range(3):
            (self.root / str(i)).touch()
        result = w.execute('inventory', {'limit': 2}, self.root)
        self.assertTrue(result['truncated'])
        self.assertEqual(len(result['files']), 2)

    def test_financial_inputs(self):
        with self.assertRaises(ValueError):
            w.execute('unit_economics', {'price': 10, 'unit_cost': -1}, self.root)
        result = w.execute('unit_economics', {'price': 10, 'unit_cost': 4}, self.root)
        self.assertEqual(result['minimum_price_for_target'], 5.72)

    def test_capacity_shortfall(self):
        result = calculate('capacity_plan', {'jobs_per_hour': 1000, 'seconds_per_job': 60,
                                            'memory_mb_per_worker': 100, 'memory_budget_mb': 500})
        self.assertTrue(result['capacity_shortfall'])
        self.assertEqual(result['feasible_workers'], 5)

    def test_replenishment(self):
        result = calculate('replenishment_plan', {'daily_demand': 10, 'lead_days': 3, 'on_hand': 20, 'coverage_days': 7})
        self.assertEqual(result['suggested_quantity'], 80)

    def test_engineering_critical_path_and_cycle(self):
        tasks = [{'id': 'a', 'duration_hours': 2}, {'id': 'b', 'duration_hours': 3},
                 {'id': 'c', 'duration_hours': 1, 'depends_on': ['a', 'b']}]
        result = calculate('engineering_plan', {'tasks': tasks})
        self.assertEqual(result['critical_path'], ['b', 'c'])
        self.assertEqual(result['minimum_elapsed_hours'], 4)
        tasks[0]['depends_on'] = ['c']
        with self.assertRaises(ValueError):
            calculate('engineering_plan', {'tasks': tasks})

    @unittest.skipUnless(os.name == 'posix', 'POSIX command runner')
    def test_command_policy_and_execution(self):
        with self.assertRaises(ValueError):
            w.execute('command', {'action': 'test'}, self.root)
        policy = self.root.parent / 'policy.json'
        policy.write_text(json.dumps({'actions': {'test': {'argv': [sys.executable, '-c', 'print("verified")']}}}))
        result = w.execute('command', {'action': 'test'}, self.root, str(policy))
        self.assertEqual(result['output'].strip(), 'verified')
        with self.assertRaises(ValueError):
            w.execute('command', {'action': 'missing'}, self.root, str(policy))

    def test_process_supervisor(self):
        for i in range(12):
            self.add(str(i))
        result = serve(self.database, self.root, concurrency=4, idle_exit=0.2)
        self.assertEqual(result['processed_attempts'], 12)
        succeeded = self.db.execute("SELECT count(*) FROM jobs WHERE state='succeeded'").fetchone()[0]
        self.assertEqual(succeeded, 12)


if __name__ == '__main__':
    unittest.main()
