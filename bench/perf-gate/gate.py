#!/usr/bin/env python3
"""The end-to-end performance gate: fastka's real producer and consumer against a real broker,
base against head, on the same runner and the same broker, runs interleaved (base, head, head,
base, ...) so drift in the machine or the broker falls on both. Noise only ever slows a run (a
neighbour's CPU, the broker's), so each side's best round is compared: the least CPU per message,
the most msgs/s; latency by its median round:

  CPU per message  (every thread of the process)   fails past +TOL_CPU
  msgs/s                                           fails past -TOL_RATE
  latency p50 / p99                                fails past +TOL_LAT (and an absolute slack)

  gate.py --head BIN [--base BIN] [--brokers 127.0.0.1:19092] [--rpk fastka-rp] [--rounds 5]

Without --base it measures head alone (a table, no verdict). Exit 1 on a regression.
"""
import argparse, json, statistics, subprocess, sys, uuid

TOL_CPU, TOL_RATE, TOL_LAT, LAT_SLACK_US = 0.10, 0.10, 0.25, 150
N = {"latency": 10_000}
N_DEFAULT = 1_000_000


def rpk(container, *args):
    subprocess.run(["docker", "exec", container, "rpk", *args], check=True, capture_output=True)


def topic(a, parts):
    t = f"gate-{uuid.uuid4().hex[:12]}"
    rpk(a.rpk, "topic", "create", t, "-p", str(parts), "-r", "1")
    return t


def run(binary, workload, brokers, t, n):
    r = subprocess.run([binary, workload, brokers, t, str(n)], capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        sys.exit(f"{binary} {workload} failed:\n{r.stderr[-3000:]}")
    return json.loads(r.stdout.strip().splitlines()[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--head", required=True)
    ap.add_argument("--base")
    ap.add_argument("--brokers", default="127.0.0.1:19092")
    ap.add_argument("--rpk", default="fastka-rp")
    ap.add_argument("--rounds", type=int, default=5)
    a = ap.parse_args()
    workloads = subprocess.run([a.head, "list"], capture_output=True, text=True, check=True).stdout.split()
    if a.base:
        have = set(subprocess.run([a.base, "list"], capture_output=True, text=True, check=True).stdout.split())
    sides = ["base", "head"] if a.base else ["head"]
    bins = {"base": a.base, "head": a.head}
    # the consume workloads read one backlog each, written once (by head), the same for both sides
    backlog = {}
    for codec in ["none", "lz4"]:
        t = topic(a, 3)
        subprocess.run([a.head, "fill", a.brokers, t, codec, str(N_DEFAULT)], check=True, timeout=600)
        backlog[f"consume_{codec}"] = t
    results = {w: {s: [] for s in sides} for w in workloads}
    for r in range(a.rounds):
        order = sides if r % 2 == 0 else sides[::-1]
        for w in workloads:
            for s in order:
                if s == "base" and w not in have:
                    continue
                n = N.get(w, N_DEFAULT)
                t = backlog.get(w) or topic(a, 1 if w == "latency" else 3)
                results[w][s].append(run(bins[s], w, a.brokers, t, n))
                if w not in backlog:
                    rpk(a.rpk, "topic", "delete", t)
        print(f"round {r + 1}/{a.rounds} done", file=sys.stderr)

    failed = []
    best = {"cpu_us_per_msg": min, "msgs_per_s": max, "p50_us": statistics.median, "p99_us": statistics.median}
    med = lambda rs, k: best[k](x[k] for x in rs)
    print(f"{'workload':<15}{'metric':<16}{'base':>12}{'head':>12}{'change':>10}")
    for w in workloads:
        metrics = [("cpu_us_per_msg", "CPU µs/msg", +1, TOL_CPU, 0), ("msgs_per_s", "msgs/s", -1, TOL_RATE, 0)]
        if w == "latency":
            metrics = [("p50_us", "p50 µs", +1, TOL_LAT, LAT_SLACK_US), ("p99_us", "p99 µs", +1, TOL_LAT, LAT_SLACK_US * 10)]
        for k, label, worse, tol, slack in metrics:
            h = med(results[w]["head"], k)
            b = med(results[w]["base"], k) if a.base and results[w]["base"] else None
            if b is None:
                print(f"{w:<15}{label:<16}{'-':>12}{h:>12,.2f}")
                continue
            d = (h - b) / b if b else 0.0
            bad = (worse > 0 and h > b * (1 + tol) + slack) or (worse < 0 and h < b * (1 - tol))
            print(f"{w:<15}{label:<16}{b:>12,.2f}{h:>12,.2f}{d:>+9.1%}{'  REGRESSION' if bad else ''}")
            if bad:
                failed.append(f"{w} {label}")
    if failed:
        sys.exit(f"\nperf gate: {', '.join(failed)} regressed")
    print("\nperf gate: ok" if a.base else "\nperf gate: head measured (no base to compare)")


if __name__ == "__main__":
    main()
