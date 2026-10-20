"""Deterministic decision support. Assumptions are supplied, never invented."""
import math


def number(data, key, default=None, positive=False):
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(key + ' must be a finite ' + ('positive' if positive else 'nonnegative') + ' number')
    return value


def calculate(capability, data):
    if capability == 'capacity_plan':
        arrival = number(data, 'jobs_per_hour')
        duration = number(data, 'seconds_per_job', positive=True)
        utilization = number(data, 'target_utilization', 0.7, positive=True)
        if utilization >= 1:
            raise ValueError('target_utilization must be below 1')
        memory = number(data, 'memory_mb_per_worker', positive=True)
        budget = number(data, 'memory_budget_mb', positive=True)
        required = max(1, math.ceil(arrival * duration / 3600 / utilization))
        fit = min(32, math.floor(budget / memory))
        workers = min(required, fit)
        return {'required_workers': required, 'feasible_workers': workers,
                'capacity_jobs_per_hour_at_target': workers * 3600 / duration * utilization,
                'capacity_shortfall': required > fit,
                'assumptions': 'Steady arrival rate, independent jobs, constant mean service time; excludes CPU/API bottlenecks and supervisor overhead.',
                'next_action': 'Reduce arrivals or add capacity; benchmark before increasing limits.' if required > fit else 'Load-test this worker count against observed latency and resource usage.'}
    if capability == 'replenishment_plan':
        demand = number(data, 'daily_demand')
        lead = number(data, 'lead_days')
        safety = number(data, 'safety_stock', 0)
        on_hand = number(data, 'on_hand')
        inbound = number(data, 'inbound', 0)
        reserved = number(data, 'reserved', 0)
        coverage = number(data, 'coverage_days', 14)
        position = on_hand + inbound - reserved
        reorder = demand * lead + safety
        target = demand * (lead + coverage) + safety
        return {'inventory_position': position, 'reorder_point': math.ceil(reorder),
                'reorder_now': position <= reorder,
                'suggested_quantity': max(0, math.ceil(target - position)) if position <= reorder else 0,
                'days_on_hand': max(0, on_hand - reserved) / demand if demand else None,
                'assumptions': 'Constant demand and lead time; inbound assumed available within planning horizon; excludes supplier minimums and cash limits.',
                'advisory_only': True}
    if capability == 'engineering_plan':
        tasks = data.get('tasks')
        if not isinstance(tasks, list) or not 1 <= len(tasks) <= 1000:
            raise ValueError('tasks must contain 1..1000 task objects')
        indexed = {}
        for task in tasks:
            if not isinstance(task, dict) or not isinstance(task.get('id'), str) or not task['id'] or task['id'] in indexed:
                raise ValueError('Every task needs a unique nonempty string id')
            number(task, 'duration_hours')
            deps = task.get('depends_on', [])
            if not isinstance(deps, list) or any(not isinstance(d, str) for d in deps):
                raise ValueError('depends_on must be a list of IDs')
            indexed[task['id']] = task
        for task in tasks:
            if any(d not in indexed for d in task.get('depends_on', [])):
                raise ValueError('Unknown dependency')
        pending = set(indexed)
        schedule, waves, paths = {}, [], {}
        while pending:
            ready = sorted(k for k in pending if all(d in schedule for d in indexed[k].get('depends_on', [])))
            if not ready:
                raise ValueError('Dependency cycle detected')
            waves.append(ready)
            for key in ready:
                task = indexed[key]
                deps = task.get('depends_on', [])
                predecessor = max(deps, key=lambda d: schedule[d]['finish_hours']) if deps else None
                start = schedule[predecessor]['finish_hours'] if predecessor else 0
                schedule[key] = {'start_hours': start, 'finish_hours': start + task['duration_hours']}
                paths[key] = (paths[predecessor] if predecessor else []) + [key]
            pending.difference_update(ready)
        last = max(schedule, key=lambda k: schedule[k]['finish_hours'])
        return {'schedule': schedule, 'dependency_waves': waves,
                'critical_path': paths[last], 'minimum_elapsed_hours': schedule[last]['finish_hours'],
                'total_effort_hours': sum(t['duration_hours'] for t in tasks),
                'assumptions': 'Unlimited resources; dependency-only lower bound, not a promised completion time. Resource locks and worker limits may extend this schedule.',
                'verification_checklist': ['Define acceptance evidence for every task', 'Identify shared resources and rollback steps', 'Validate critical-path assumptions against measurements', 'Record actual duration and update estimates']}
    raise ValueError('Unknown planning capability')
