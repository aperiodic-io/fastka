#!/usr/bin/env python3
"""The benchmark gate: each workload of benches/gate.rs counted in instructions (Valgrind's
cachegrind, no cache simulation), less `noop`'s, so a count is the workload's own and stable to
well under 0.1% on any machine with the same toolchain.

  gate.py HEAD_BIN                      counts, against bench/gate/baseline.json (same toolchain)
  gate.py HEAD_BIN --base BASE_BIN      head against base, built in the same job (CI)
  gate.py HEAD_BIN --write-baseline     records bench/gate/baseline.json

Fails (exit 1) when a workload takes more than --tolerance (default 3%) instructions more.
"""
import argparse, json, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
BASELINE = os.path.join(HERE, "baseline.json")


def count(binary, workload):
    r = subprocess.run(
        ["valgrind", "--tool=cachegrind", "--cache-sim=no", "--cachegrind-out-file=/dev/null", binary, workload],
        capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"{binary} {workload} failed:\n{r.stderr[-2000:]}")
    m = re.search(r"I\s+refs:\s+([\d,]+)", r.stderr)
    return int(m.group(1).replace(",", ""))


def counts(binary):
    names = subprocess.run([binary, "list"], capture_output=True, text=True, check=True).stdout.split()
    noop = count(binary, "noop")
    return {n: count(binary, n) - noop for n in names if n != "noop"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("head")
    ap.add_argument("--base")
    ap.add_argument("--write-baseline", action="store_true")
    ap.add_argument("--tolerance", type=float, default=0.03)
    a = ap.parse_args()
    head = counts(a.head)
    if a.write_baseline:
        json.dump(head, open(BASELINE, "w"), indent=2, sort_keys=True)
        print(f"wrote {BASELINE}")
        return
    base = counts(a.base) if a.base else json.load(open(BASELINE))
    failed = []
    print(f"{'workload':<16}{'base':>16}{'head':>16}{'change':>10}")
    for n, h in sorted(head.items()):
        b = base.get(n)
        if b is None:
            print(f"{n:<16}{'(new)':>16}{h:>16,}")
            continue
        d = (h - b) / b
        mark = "  REGRESSION" if d > a.tolerance else ""
        print(f"{n:<16}{b:>16,}{h:>16,}{d:>+9.2%}{mark}")
        if mark:
            failed.append(n)
    if failed:
        sys.exit(f"\ngate: {', '.join(failed)} over +{a.tolerance:.0%} instructions")
    print("\ngate: ok")


if __name__ == "__main__":
    main()
