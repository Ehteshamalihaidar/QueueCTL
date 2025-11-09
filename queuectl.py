#!/usr/bin/env python3
"""
queuectl.py - CLI for the QueueCTL assignment
"""

import argparse
import json
import os
import sys
import uuid
import time
from storage import Storage
from worker import start_workers, stop_workers
from datetime import datetime, timezone

CONFIG_PATH = os.path.join(os.getcwd(), "config.json")

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    default = {"max_retries": 3, "backoff_base": 2}
    with open(CONFIG_PATH, "w") as f:
        json.dump(default, f, indent=2)
    return default

def cmd_enqueue(args):
    storage = Storage()
    raw = args.payload
    try:
        job = json.loads(raw)
    except Exception:
        print("Invalid JSON payload")
        return
    # enforce required fields
    job_id = job.get("id", f"job-{uuid.uuid4().hex[:8]}")
    job['id'] = job_id
    job['command'] = job.get("command", "")
    job['state'] = job.get("state", "pending")
    job['attempts'] = int(job.get("attempts", 0))
    job['max_retries'] = int(job.get("max_retries", load_config().get("max_retries", 3)))
    now = datetime.now(timezone.utc).isoformat()
    job['created_at'] = job.get("created_at", now)
    job['updated_at'] = job.get("updated_at", now)
    try:
        storage.add_job(job)
        print(f"Enqueued job {job_id}")
    except Exception as e:
        print(f"Failed to enqueue: {e}")

def cmd_worker_start(args):
    count = args.count or 1
    daemon = args.daemon
    print(f"Starting {count} worker(s) {'as daemon' if daemon else 'in foreground'}")
    start_workers(count=count, daemon=daemon)

def cmd_worker_stop(args):
    stop_workers()

def cmd_status(args):
    storage = Storage()
    summary = storage.summary()
    total = sum(summary.values()) if summary else 0
    print("Job summary:")
    for state in ['pending','processing','completed','failed','dead']:
        print(f"  {state:10} : {summary.get(state,0)}")
    # show active workers via pidfile
    pidfile = os.path.join(os.getcwd(), "queuectl_worker.pid")
    if os.path.exists(pidfile):
        with open(pidfile,"r") as f:
            pid = f.read().strip()
        print(f"Worker manager pidfile: {pidfile} (pid {pid})")
    else:
        print("No worker manager pidfile found (workers may be running in foreground)")

def cmd_list(args):
    storage = Storage()
    jobs = storage.list_jobs(state=args.state)
    if not jobs:
        print("No jobs")
        return
    for j in jobs:
        print(json.dumps(j, indent=2))

def cmd_dlq_list(args):
    storage = Storage()
    dlq_jobs = storage.list_jobs(state='dead')
    if not dlq_jobs:
        print("DLQ empty")
        return
    for j in dlq_jobs:
        print(json.dumps(j, indent=2))

def cmd_dlq_retry(args):
    storage = Storage()
    job = storage.get_job(args.job_id)
    if not job:
        print("Job not found")
        return
    if job['state'] != 'dead':
        print("Job is not in DLQ (state != dead)")
        return
    # reset attempts and state
    storage.move_to_state(job_id=args.job_id, new_state='pending', last_error=None)
    # reset attempts to 0
    conn = storage._conn()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute("UPDATE jobs SET attempts=0, updated_at=? WHERE id=?", (now, args.job_id))
    conn.close()
    print(f"Re-queued job {args.job_id} from DLQ")

def cmd_config_set(args):
    cfg = load_config()
    key = args.key
    val = args.value
    # cast to int if numeric
    try:
        if '.' in val:
            cast = float(val)
        else:
            cast = int(val)
        val = cast
    except Exception:
        # keep string
        pass
    cfg[key] = val
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
    print(f"Set config {key}={val}")

def cmd_config_get(args):
    cfg = load_config()
    if args.key:
        print(json.dumps({args.key: cfg.get(args.key)}, indent=2))
    else:
        print(json.dumps(cfg, indent=2))

def main():
    parser = argparse.ArgumentParser(prog="queuectl", description="QueueCTL - simple job queue CLI")
    sub = parser.add_subparsers(dest="cmd")

    # enqueue
    p = sub.add_parser("enqueue", help="Enqueue a job using JSON payload")
    p.add_argument("payload", help='JSON payload, e.g. \'{"id":"job1","command":"sleep 2"}\'')
    p.set_defaults(func=cmd_enqueue)

    # worker start
    p = sub.add_parser("worker", help="Worker management")
    wp = p.add_subparsers(dest="subcmd")
    ps = wp.add_parser("start", help="Start worker(s)")
    ps.add_argument("--count", type=int, default=1, help="Number of worker processes")
    ps.add_argument("--daemon", action="store_true", help="Run manager as daemon (pidfile created)")
    ps.set_defaults(func=cmd_worker_start)
    ps2 = wp.add_parser("stop", help="Stop worker manager (if started as daemon)")
    ps2.set_defaults(func=cmd_worker_stop)

    # status
    p = sub.add_parser("status", help="Show summary of job states and worker info")
    p.set_defaults(func=cmd_status)

    # list
    p = sub.add_parser("list", help="List jobs optionally by state")
    p.add_argument("--state", type=str, help="Filter by state (pending,processing,completed,failed,dead)")
    p.set_defaults(func=cmd_list)

    # dlq
    p = sub.add_parser("dlq", help="Dead Letter Queue operations")
    dp = p.add_subparsers(dest="dlqcmd")
    dpl = dp.add_parser("list", help="List DLQ jobs")
    dpl.set_defaults(func=cmd_dlq_list)
    dpr = dp.add_parser("retry", help="Retry a DLQ job")
    dpr.add_argument("job_id", help="job id to retry")
    dpr.set_defaults(func=cmd_dlq_retry)

    # config
    p = sub.add_parser("config", help="Config management")
    cp = p.add_subparsers(dest="cfgcmd")
    cps = cp.add_parser("set", help="Set a config value")
    cps.add_argument("key", help="config key (max_retries, backoff_base)")
    cps.add_argument("value", help="value")
    cps.set_defaults(func=cmd_config_set)
    cpg = cp.add_parser("get", help="Get config")
    cpg.add_argument("key", nargs='?', help="optional key")
    cpg.set_defaults(func=cmd_config_get)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)

if __name__ == "__main__":
    main()
