#!/usr/bin/env python3
"""Profile a fresh minprof build with stage timestamps, RSS, and scratch bytes.

This wraps the release binary without changing its output or index format.
RSS and disk are sampled, so stage peaks are lower bounds. The process-wide
maximum RSS comes from wait4 and is exact to the OS's reporting granularity.
"""

import argparse
import csv
import json
import os
import platform
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from queue import Empty, Queue


PHASE_MARKERS = (
    ("=== pass 1:", "pass1"),
    ("=== pass 2:", "pass2"),
    ("=== pass 3:", "pass3"),
    ("loading object index...", "pass3/load_ids"),
    ("resolving GC root node indices...", "pass3/roots"),
    ("building adjacency lists...", "pass3/adjacency"),
    ("computing DFS spanning tree...", "pass3/dfs"),
    ("building node_to_pre...", "pass3/node_to_pre"),
    ("sorting predecessor edges by DFS preorder...", "pass3/pred_sort"),
    ("computing dominators (Semi-NCA)...", "pass3/semi_nca"),
    ("converting idom to RPO...", "pass3/idom_conversion"),
    ("=== pass 4:", "pass4"),
    ("=== done", "build_done"),
    ("=== query ===", "query"),
)


def phase_for_line(line: str) -> str | None:
    line = line.strip()
    for prefix, phase in PHASE_MARKERS:
        if line.startswith(prefix):
            return phase
    return None


def rss_bytes(pid: int) -> int:
    try:
        result = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True
        )
    except OSError as error:
        raise RuntimeError(f"ps cannot run: {error}") from error
    if result.returncode or not result.stdout.strip():
        raise RuntimeError(f"ps could not read RSS for PID {pid}: {result.stderr.strip()}")
    return int(result.stdout.strip()) * 1024  # ps reports KiB on macOS and Linux


def index_bytes(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(path.stat().st_size for path in directory.iterdir() if path.is_file())


def system_swap_used_mib() -> float | None:
    """System-wide snapshot; other processes can change it during the run."""
    if sys.platform == "darwin":
        result = subprocess.run(["sysctl", "vm.swapusage"], capture_output=True, text=True)
        match = re.search(r"used = ([0-9.]+)M", result.stdout)
        return float(match.group(1)) if match else None
    if sys.platform.startswith("linux"):
        try:
            fields = {
                line.split(":", 1)[0]: int(line.split()[1])
                for line in Path("/proc/meminfo").read_text().splitlines()
                if line.startswith(("SwapTotal:", "SwapFree:"))
            }
            return (fields["SwapTotal"] - fields["SwapFree"]) / 1024
        except (OSError, KeyError, ValueError):
            return None
    return None


def read_stderr(stream, path: Path, events: Queue) -> None:
    with path.open("wb") as output:
        for line in stream:
            timestamp = time.perf_counter()
            output.write(line)
            output.flush()
            events.put((timestamp, line.decode("utf-8", errors="replace")))
    events.put((time.perf_counter(), None))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dump", type=Path, help="existing HPROF file")
    parser.add_argument("run_dir", type=Path, help="new output directory")
    parser.add_argument("--binary", type=Path, default=Path("target/release/minprof"))
    parser.add_argument("--interval-ms", type=int, default=250)
    parser.add_argument("--sort-mib", type=int,
                        help="override sort chunk size in MiB for this benchmark")
    args = parser.parse_args()
    if args.interval_ms < 50:
        parser.error("--interval-ms must be at least 50")
    if args.sort_mib is not None and args.sort_mib < 1:
        parser.error("--sort-mib must be positive")
    dump = args.dump.resolve()
    binary = args.binary.resolve()
    if not dump.is_file() or not binary.is_file():
        parser.error("dump and --binary must be existing files")
    try:
        rss_bytes(os.getpid())
    except RuntimeError as error:
        parser.error(str(error))

    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    index = run_dir / "index"
    events: Queue = Queue()
    samples = []
    transitions = []
    phase = "startup"
    swap_used_before_mib = system_swap_used_mib()
    start = time.perf_counter()
    command = [str(binary), "-p", str(dump), "-o", str(index), "--format", "json"]
    child_env = os.environ.copy()
    if args.sort_mib is not None:
        child_env["MINPROF_BENCH_SORT_MIB"] = str(args.sort_mib)

    with (run_dir / "report.json").open("wb") as report:
        child = subprocess.Popen(command, stdout=report, stderr=subprocess.PIPE, env=child_env)
        assert child.stderr is not None
        reader = threading.Thread(
            target=read_stderr,
            args=(child.stderr, run_dir / "minprof.stderr", events),
            daemon=True,
        )
        reader.start()
        status = None
        usage = None
        while status is None:
            while True:
                try:
                    timestamp, line = events.get_nowait()
                except Empty:
                    break
                if line is None:
                    continue
                marker = phase_for_line(line)
                if marker is not None:
                    phase = marker
                    transitions.append({"seconds": timestamp - start, "phase": phase})
            try:
                resident = rss_bytes(child.pid)
            except RuntimeError:
                resident = None  # process may have just exited
            samples.append({
                "seconds": round(time.perf_counter() - start, 4),
                "phase": phase,
                "rss_bytes": resident,
                "index_bytes": index_bytes(index),
            })
            waited_pid, status, usage = os.wait4(child.pid, os.WNOHANG)
            if waited_pid == 0:
                status = None
                time.sleep(args.interval_ms / 1000)

        child.returncode = os.waitstatus_to_exitcode(status)
        reader.join()
        while True:
            try:
                timestamp, line = events.get_nowait()
            except Empty:
                break
            if line is not None:
                marker = phase_for_line(line)
                if marker is not None:
                    phase = marker
                    transitions.append({"seconds": timestamp - start, "phase": phase})

    wall = time.perf_counter() - start
    swap_used_after_mib = system_swap_used_mib()
    maxrss = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
    phase_peaks = {}
    pass_peaks = {}
    for sample in samples:
        peak = phase_peaks.setdefault(sample["phase"], {"rss_bytes": 0, "index_bytes": 0})
        peak["rss_bytes"] = max(peak["rss_bytes"], sample["rss_bytes"] or 0)
        peak["index_bytes"] = max(peak["index_bytes"], sample["index_bytes"])
        pass_name = sample["phase"].split("/")[0]
        pass_peak = pass_peaks.setdefault(pass_name, {"rss_bytes": 0, "index_bytes": 0})
        pass_peak["rss_bytes"] = max(pass_peak["rss_bytes"], sample["rss_bytes"] or 0)
        pass_peak["index_bytes"] = max(pass_peak["index_bytes"], sample["index_bytes"])
    durations = {}
    for current, following in zip(transitions, transitions[1:]):
        durations[current["phase"]] = round(
            following["seconds"] - current["seconds"], 4
        )
    if transitions:
        durations[transitions[-1]["phase"]] = round(wall - transitions[-1]["seconds"], 4)
    pass_starts = [event for event in transitions if event["phase"] in
                   ("pass1", "pass2", "pass3", "pass4", "build_done", "query")]
    pass_seconds = {
        current["phase"]: round(following["seconds"] - current["seconds"], 4)
        for current, following in zip(pass_starts, pass_starts[1:])
    }
    if pass_starts:
        pass_seconds[pass_starts[-1]["phase"]] = round(wall - pass_starts[-1]["seconds"], 4)

    summary = {
        "command": command,
        "sort_mib": args.sort_mib,
        "host": platform.platform(),
        "dump_bytes": dump.stat().st_size,
        "returncode": child.returncode,
        "wall_seconds": round(wall, 4),
        "process_maxrss_bytes": maxrss,
        "process_cpu_user_seconds": round(usage.ru_utime, 4),
        "process_cpu_system_seconds": round(usage.ru_stime, 4),
        "process_major_faults": usage.ru_majflt,
        "process_minor_faults": usage.ru_minflt,
        "process_reported_swaps": usage.ru_nswap,
        "system_swap_used_before_mib": swap_used_before_mib,
        "system_swap_used_after_mib": swap_used_after_mib,
        "sampled_peak_index_bytes": max(s["index_bytes"] for s in samples),
        "final_index_bytes": index_bytes(index),
        "sample_interval_ms": args.interval_ms,
        "phase_seconds": durations,
        "phase_sampled_peaks": phase_peaks,
        "pass_seconds": pass_seconds,
        "pass_sampled_peaks": pass_peaks,
        "transitions": transitions,
    }
    with (run_dir / "summary.json").open("w") as output:
        json.dump(summary, output, indent=2)
        output.write("\n")
    with (run_dir / "samples.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=samples[0].keys())
        writer.writeheader()
        writer.writerows(samples)
    print(json.dumps({k: v for k, v in summary.items() if k != "transitions"}, indent=2))
    if child.returncode:
        raise SystemExit(child.returncode)


if __name__ == "__main__":
    main()
