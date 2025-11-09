#!/usr/bin/env python3
"""
worker.py - Windows-safe worker loop for queuectl
"""

import os
import signal
import subprocess
import time
import json
from multiprocessing import Process, current_process
from math import pow
from typing import Optional
from storage import Storage

PIDFILE = os.path.join(os.getcwd(), "queuectl_worker.pid")
CONFIG_PATH = os.path.join(os.getcwd(), "config.json")

_running = True


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {"max_retries": 3, "backoff_base": 2}


def _handle_sigint(signum, frame):
    global _running
    _running = False
    print(f"[{current_process().name}] Received shutdown signal, finishing current job and exiting...")


def run_worker_loop(worker_id: int, poll_interval: float = 1.0):
    """
    Windows-safe worker loop — creates fresh SQLite connections for each operation.
    """
    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    cfg = load_config()
    base = cfg.get("backoff_base", 2)
    print(f"[worker-{worker_id}] started (pid={os.getpid()})")

    while _running:
        try:
            storage = Storage()  # create fresh connection every loop
            job = storage.fetch_and_lock_next_job()
        except Exception as e:
            print(f"[worker-{worker_id}] Error fetching job: {e}")
            time.sleep(poll_interval)
            continue

        if not job:
            time.sleep(poll_interval)
            continue

        job_id = job["id"]
        attempts = job["attempts"]
        max_retries = job["max_retries"]

        try:
            locked = storage.increment_attempts_and_lock(job_id)
        except Exception as e:
            print(f"[worker-{worker_id}] Lock failed for job {job_id}: {e}")
            continue

        if not locked:
            continue

        cmd = job["command"]
        print(f"[worker-{worker_id}] Processing job {job_id}: {cmd} (attempt {locked['attempts']})")
        start_time = time.time()

        success = True
        err = None
        try:
            proc = subprocess.run(cmd, shell=True)
            if proc.returncode != 0:
                success = False
                err = f"exit_code={proc.returncode}"
        except Exception as e:
            success = False
            err = str(e)

        attempts_after = locked["attempts"]
        try:
            storage.update_job_after_execution(
                job_id=job_id,
                success=success,
                attempts=attempts_after,
                max_retries=max_retries,
                err=err,
            )
        except Exception as e:
            print(f"[worker-{worker_id}] Failed to update job {job_id}: {e}")
            continue
        finally:
            del storage  # ensure DB connection is closed before next loop

        elapsed = time.time() - start_time
        if success:
            print(f"[worker-{worker_id}] Job {job_id} completed in {elapsed:.2f}s")
        else:
            if attempts_after >= max_retries:
                print(f"[worker-{worker_id}] Job {job_id} failed permanently -> DLQ (attempts={attempts_after}) last_err={err}")
            else:
                backoff = pow(base, attempts_after)
                print(f"[worker-{worker_id}] Job {job_id} failed (attempts={attempts_after}), will retry after ~{int(backoff)}s: last_err={err}")
                time.sleep(min(backoff, 5))

    print(f"[worker-{worker_id}] exiting")


def start_workers(count: int, daemon: bool = False):
    """Start multiple workers safely on Windows."""
    procs = []
    if daemon and os.name == "nt":
        print("⚠️  Daemon mode not supported on Windows. Running in foreground instead.")
        daemon = False

    if daemon:
        pid = os.fork()
        if pid > 0:
            with open(PIDFILE, "w") as f:
                f.write(str(pid))
            print(f"Worker manager started as daemon (pidfile={PIDFILE})")
            return

    try:
        for i in range(count):
            p = Process(target=run_worker_loop, args=(i + 1,))
            p.start()
            procs.append(p)
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("Shutdown requested. Stopping workers...")
        for p in procs:
            if p.is_alive():
                p.terminate()
        for p in procs:
            p.join()


def stop_workers():
    """Stop daemon workers (Linux/Mac only)."""
    if not os.path.exists(PIDFILE):
        print("No pidfile found; workers not started as daemon or already stopped.")
        return
    with open(PIDFILE, "r") as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to pid {pid}")
    except ProcessLookupError:
        print("Process not found.")
    os.remove(PIDFILE)
