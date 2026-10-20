# Workhorse execution engine

A local, durable execution sidecar for an assistant: the assistant plans and reviews;
Workhorse schedules, executes, records evidence, and reports failures. Python 3.11+
standard library only. The original Gradle Play Publisher remains intact.

## What works

- Concurrent process execution: 8 slots by default, configurable from 1 to 32.
- SQLite WAL queue, 10,000 pending-job default admission limit, atomic claims,
  idempotency keys, bounded retries, exponential backoff, expired-lease recovery.
- Priorities, dependency ordering and failure propagation, exclusive resource keys.
- Durable task-boundary checkpoints, results, errors and timestamped event history.
- Supervisor timeouts, crash recovery and graceful shutdown (stop admitting work,
  drain active jobs). Cancellation fences results; it does not immediately interrupt
  an already executing handler.
- Operator-allowlisted commands for builds, tests and other approved automation.
- Inventory, JSON validation with content digest, unit economics, worker capacity,
  stock replenishment, and engineering dependency/critical-path calculations.

These are implemented capabilities, not an autonomous general intelligence. No LLM,
cloud account, Shopify API, Google Play account, or ChatGPT plugin is connected by
this change. The host assistant supplies the reasoning and translates a mission
into validated tasks. There are no recurring charges or live price mutations.

## Quick start

Run from the repository root:

```sh
python worker/workhorse.py capabilities
python worker/workhorse.py --db /tmp/workhorse.sqlite3 submit inventory --key repo-inventory-1 --payload '{"path":".","limit":1000}'
python worker/workhorse.py --db /tmp/workhorse.sqlite3 serve --root "$PWD" --concurrency 8 --idle-exit 3
python worker/workhorse.py --db /tmp/workhorse.sqlite3 metrics
python worker/workhorse.py --db /tmp/workhorse.sqlite3 status JOB_ID
python worker/workhorse.py --db /tmp/workhorse.sqlite3 events JOB_ID
```

For durable service use, put the database on a persistent local disk, outside the
working repository. Run `serve` without `--idle-exit` under your host's service
manager. This command runs until stopped; nothing installs or starts it implicitly.
Use a dedicated unprivileged account. Keep one supervisor per database, one
workspace root and one operator policy; do not point it at different roots.
SQLite is a single-host queue, **not** a multi-machine broker. Back up the database
using SQLite's backup API while running. Jobs and results may contain sensitive
business inputs: restrict database permissions and set your retention policy.

## Delegation protocol

1. Specify the objective, authorized systems, acceptance evidence and cost/time
   limits. Inspect `capabilities`; never invent an available action.
2. Break the mission into small jobs. Mark dependencies and shared resources.
   A dependency must already exist, preventing submission-time cycles.
3. Submit with stable request keys. Reusing a key with different inputs is rejected.
   Submit upstream jobs first; pass their IDs with repeated `--depends-on` flags.
4. Independent jobs run together. Use `--resource repo:NAME` for work that shares
   mutable files; same-resource jobs serialize. Command jobs default to a single
   workspace resource lock. Only use separate keys for genuinely isolated work.
5. Check status and evidence. `succeeded` means that the handler completed its
   defined checks, not that an entire business objective was achieved.
6. Retry transient failures; fix deterministic failures with a new task/key.
   Downstream tasks become `blocked` after failed/cancelled dependencies.
7. Report facts, assumptions, failed checks, artifacts and remaining blockers.
   Preserve original approvals; delegation does not expand permissions.

Example scheduling:

```sh
python worker/workhorse.py submit validate_json --key config-check-1 --payload '{"path":"config.json"}' --priority 10
python worker/workhorse.py submit inventory --key after-config-1 --payload '{}' --depends-on RETURNED_JOB_ID
```

`run --root PATH` handles one job; `serve` continuously fills available slots.
`cancel JOB_ID` prevents queued execution or rejects the running job's eventual
result. It cannot undo side effects already performed. Retry execution is
**at-least-once**, not exactly-once. All external writes need idempotency or a
reconciliation step. State fencing protects queue results, not external services.

## Engineering and logistics

| Capability | Required inputs | Result |
| --- | --- | --- |
| `capacity_plan` | `jobs_per_hour`, `seconds_per_job`, `memory_mb_per_worker`, `memory_budget_mb` | Required/feasible workers, estimated throughput, bottleneck flag |
| `replenishment_plan` | `daily_demand`, `lead_days`, `on_hand` | Inventory position, reorder point, quantity and coverage |
| `engineering_plan` | `tasks`: unique `id`, `duration_hours`, optional `depends_on` | Dependency waves, critical path, earliest schedule and effort |
| `unit_economics` | `price`, `unit_cost` | Contribution, margin, price needed for target margin |

Optional inputs include `target_utilization` (0.7), `safety_stock`, `inbound`,
`reserved`, `coverage_days` (14), `fee_rate`, `fixed_fee`, `target_margin` (0.3),
and `currency` (USD). Inputs must share consistent units/currency. Estimates are
conditional on supplied inputs; outputs explain their simplifying assumptions.
Pricing calculations are advisory and do not edit stores or payment systems.

Planning discipline: define constraints before optimizing; measure arrivals and
service time; distinguish demand from available supply; identify the critical
path; isolate failure domains; choose a rollback; test acceptance conditions;
compare estimates to observed outcomes. The engineering planner assumes unlimited
resources, so its duration is a lower bound rather than a deadline promise.

```sh
python worker/workhorse.py submit engineering_plan --key launch-plan-1 --payload '{"tasks":[{"id":"audit","duration_hours":2},{"id":"tests","duration_hours":1},{"id":"review","duration_hours":1,"depends_on":["audit","tests"]}]}'
```

## Build and test commands

Create an operator-owned policy **outside** the workspace. Example (adjust the
absolute interpreter path for your host):

```json
{
  "actions": {
    "worker-tests": {
      "argv": ["/usr/bin/python3", "-m", "unittest", "discover", "-s", "worker/tests", "-v"],
      "timeout_seconds": 120
    }
  }
}
```

```sh
python worker/workhorse.py submit command --key worker-test-1 --payload '{"action":"worker-tests"}'
python worker/workhorse.py --policy /path/outside/repo/policy.json serve --root "$PWD" --concurrency 8 --idle-exit 3
```

Task payloads cannot supply executables, shell strings, flags or environment
variables. Commands use fixed argv, minimal environment, closed stdin, a timeout
(up to 180 seconds), process-group cleanup, 2 MiB file-size limit and 32 KiB
captured output. Nonzero exit codes fail the job. The file-size limit also applies
to command-created files: larger build artifacts need a separately reviewed adapter.
The command adapter currently requires POSIX. Policies authorize executable code,
including scripts in the repo; only run trusted, reviewed code. This is **not a
security sandbox**: use an OS/container sandbox for untrusted repositories.
Avoid secrets in command output. No credentials are passed through automatically.

## Verification and limits

```sh
python -m unittest discover -s worker/tests -v
python worker/benchmark.py --jobs 200 --workers 8
```

Tests cover concurrent unique claims, leases, stale-result rejection, resource
locks, cancellation, retries, dependency failure, backpressure, path boundaries,
commands, planning and real process execution. Benchmark output reports task count,
wall time, attempts and outcome counts; it is a small synthetic workload, not a
promise of build, AI or API throughput. More processes do not imply more capacity:
RAM, CPU, disk and downstream quotas still apply. No GPU/distributed autoscaling,
vector memory, model inference, intra-job resume or production deployment is
included. Priority scheduling may starve lower-priority jobs under a sustained
high-priority stream. Task leases are five minutes and supervisor timeout is four;
long work must be split into bounded jobs. `--idle-exit` may leave delayed retries
queued; use continuous service mode to process those automatically.
