#!/usr/bin/env python3
"""
run_ci.py - Run GitHub Actions workflow jobs locally from rust.yml.

Requires: pyyaml (pip install pyyaml), podman

Usage:
    python3 run_ci.py                          # run all jobs
    python3 run_ci.py test_maria doc_check     # run specific jobs
    python3 run_ci.py --list                   # list available jobs
    python3 run_ci.py --workflow path/to.yml   # use a different workflow file
"""

import argparse
import os
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any

import yaml

WORKFLOW_FILE = ".github/workflows/rust.yml"

# Track running containers so we can clean up on SIGINT/SIGTERM.
_running_containers: list[str] = []


def _cleanup_and_exit(signum: int, frame: Any) -> None:  # noqa: ARG001
    print("\n[interrupted] Stopping containers...", flush=True)
    for name in list(_running_containers):
        _stop_container(name)
    sys.exit(130)


signal.signal(signal.SIGINT, _cleanup_and_exit)
signal.signal(signal.SIGTERM, _cleanup_and_exit)


def job_needs_venv(job: dict[str, Any]) -> bool:
    """Return True if any step in the job runs pip install."""
    for step in job.get("steps", []):
        run = step.get("run", "")
        if "pip install" in run or "pip3 install" in run:
            return True
    return False


def make_venv(tmpdir: str) -> str:
    """Create a venv in tmpdir and return its bin directory."""
    venv_dir = os.path.join(tmpdir, "venv")
    subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
    return os.path.join(venv_dir, "bin")


def run_step(cmd: str, env: dict[str, str]) -> bool:
    result = subprocess.run(["bash", "-e", "-c", cmd], env=env)
    return result.returncode == 0


def _stop_container(name: str) -> None:
    subprocess.run(["podman", "rm", "-f", name], capture_output=True)
    if name in _running_containers:
        _running_containers.remove(name)


def start_service(job_name: str, svc_name: str, svc: dict[str, Any]) -> bool:
    container_name = f"run_ci_{job_name}_{svc_name}"
    # Remove any pre-existing container with this name
    subprocess.run(["podman", "rm", "-f", container_name], capture_output=True)

    args: list[str] = ["podman", "run", "-d", "--name", container_name]

    for k, v in svc.get("env", {}).items():
        args += ["-e", f"{k}={v}"]

    for port in svc.get("ports", []):
        args += ["-p", str(port)]

    for mount in svc.get("volumes", []):
        host_dir = str(mount).split(":")[0]
        os.makedirs(host_dir, exist_ok=True)
        args += ["-v", str(mount)]

    options = svc.get("options", "")
    if options:
        # Strip health-check flags: podman supports them but they differ slightly;
        # we handle health polling ourselves in wait_healthy().
        filtered = [
            tok for tok in shlex.split(options) if not tok.startswith("--health")
        ]
        args += filtered

    args.append(svc["image"])

    print(f"    podman run {' '.join(args[4:])}", flush=True)
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"    ERROR: {result.stderr.strip()}", flush=True)
        return False
    _running_containers.append(container_name)
    return True


def wait_healthy(
    job_name: str, svc_name: str, svc: dict[str, Any], timeout: int = 120
) -> bool:
    container_name = f"run_ci_{job_name}_{svc_name}"
    # Extract --health-cmd value from options so we can poll it ourselves.
    options = svc.get("options", "")
    health_cmd: str | None = None
    if options:
        toks = shlex.split(options)
        for i, tok in enumerate(toks):
            if tok.startswith("--health-cmd="):
                health_cmd = tok.split("=", 1)[1]
                break
            if tok == "--health-cmd" and i + 1 < len(toks):
                health_cmd = toks[i + 1]
                break

    deadline = time.time() + timeout
    while time.time() < deadline:
        if health_cmd:
            r = subprocess.run(
                ["podman", "exec", container_name, "bash", "-c", health_cmd],
                capture_output=True,
            )
            if r.returncode == 0:
                return True
        else:
            # No health command: just check the container is still running.
            r = subprocess.run(
                [
                    "podman",
                    "inspect",
                    "--format",
                    "{{.State.Running}}",
                    container_name,
                ],
                capture_output=True,
                text=True,
            )
            if r.stdout.strip() == "true":
                return True
        time.sleep(3)
    print(f"    Timeout waiting for {container_name} to be ready", flush=True)
    return False


def stop_services(job_name: str, service_names: list[str]) -> None:
    for svc_name in service_names:
        _stop_container(f"run_ci_{job_name}_{svc_name}")


def run_job(job_name: str, job: dict[str, Any], global_env: dict[str, str]) -> bool:
    print(f"\n{'=' * 60}", flush=True)
    print(f"JOB: {job_name}", flush=True)
    print(f"{'=' * 60}", flush=True)

    services: dict[str, Any] = job.get("services", {})

    for svc_name, svc in services.items():
        print(f"\n  [service] Starting {svc_name} ({svc['image']})...", flush=True)
        if not start_service(job_name, svc_name, svc):
            stop_services(job_name, list(services.keys()))
            return False
        if "options" in svc and "--health-cmd" in svc["options"]:
            print(f"  [service] Waiting for {svc_name} to be healthy...", flush=True)
            if not wait_healthy(job_name, svc_name, svc):
                stop_services(job_name, list(services.keys()))
                return False
            print(f"  [service] {svc_name} is healthy.", flush=True)

    success = True
    try:
        # Work on a per-job copy so venv PATH changes don't leak to other jobs.
        job_env = dict(global_env)
        venv_bin: str | None = None
        tmpdir_obj = None
        if job_needs_venv(job):
            tmpdir_obj = tempfile.TemporaryDirectory()
            venv_bin = make_venv(tmpdir_obj.name)
            print(f"  [venv] Created temporary venv at {venv_bin}", flush=True)
            # Prepend venv bin so pip/python/ruff etc. resolve from the venv
            job_env["PATH"] = (
                venv_bin + ":" + job_env.get("PATH", os.environ.get("PATH", ""))
            )
            job_env["VIRTUAL_ENV"] = os.path.dirname(venv_bin)
            job_env.pop("PYTHONHOME", None)

        for step in job.get("steps", []):
            step_name = step.get("name", "(unnamed)")
            print(f"\n  --- {step_name} ---", flush=True)

            if "uses" in step:
                print(f"    (skip: uses: {step['uses']})", flush=True)
                continue

            if "run" not in step:
                continue

            step_env = {**job_env}
            for k, v in step.get("env", {}).items():
                step_env[k] = str(v)

            if not run_step(step["run"], step_env):
                print(f"\n  FAILED: {step_name}", flush=True)
                success = False
                break
    finally:
        stop_services(job_name, list(services.keys()))
        if tmpdir_obj is not None:
            tmpdir_obj.cleanup()

    print(f"\nJob {job_name}: {'PASSED' if success else 'FAILED'}", flush=True)
    return success


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run GitHub Actions CI jobs locally from a workflow YAML file."
    )
    parser.add_argument("jobs", nargs="*", help="Job names to run (default: all)")
    parser.add_argument("--workflow", default=WORKFLOW_FILE, help="Workflow YAML file")
    parser.add_argument(
        "--list", action="store_true", help="List available jobs and exit"
    )
    args = parser.parse_args()

    with open(args.workflow) as f:
        workflow: dict[str, Any] = yaml.safe_load(f)

    all_jobs: dict[str, Any] = workflow.get("jobs", {})

    if args.list:
        for name in all_jobs:
            print(name)
        return

    selected: list[str] = args.jobs if args.jobs else list(all_jobs.keys())
    unknown = [j for j in selected if j not in all_jobs]
    if unknown:
        print(f"Unknown jobs: {', '.join(unknown)}", file=sys.stderr)
        print(f"Available: {', '.join(all_jobs.keys())}", file=sys.stderr)
        sys.exit(1)

    global_env: dict[str, str] = {**os.environ}
    for k, v in workflow.get("env", {}).items():
        global_env[k] = str(v)

    results: dict[str, bool] = {}
    for job_name in selected:
        results[job_name] = run_job(job_name, all_jobs[job_name], global_env)

    print(f"\n{'=' * 60}", flush=True)
    print("Summary:", flush=True)
    for job_name, ok in results.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {job_name}", flush=True)

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
