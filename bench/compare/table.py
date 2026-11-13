#!/usr/bin/env python3
"""bench/compare/run.sh's JSON lines → the README's tables: the median of each (workload, codec,
client) over its runs; msgs/s from wall time, CPU µs per message (every thread of the process),
peak RSS. A client whose runs failed is shown with why."""
import json, statistics, sys
from collections import defaultdict

rows = defaultdict(list)
errors = {}
for line in open(sys.argv[1]):
    line = line.strip()
    if not line.startswith("{"):
        continue
    r = json.loads(line)
    k = (r["workload"], r["codec"], r["client"])
    if "error" in r:
        errors[k] = r["error"]
    else:
        rows[k].append(r)

order = ["fastka", "rdkafka", "rskafka", "kafka-rust"]
for workload in ["consume", "produce"]:
    for codec in ["none", "lz4"]:
        keys = [k for k in list(rows) + list(errors) if k[0] == workload and k[1] == codec]
        if not keys:
            continue
        n = next((rs[0]["n"] for k, rs in rows.items() if k[:2] == (workload, codec)), 0)
        size = next((rs[0]["size"] for k, rs in rows.items() if k[:2] == (workload, codec)), 0)
        print(f"\n**{workload.capitalize()}, {n:,} messages of {size} B, compression: {codec}**\n")
        print("| client | msgs/s | CPU µs / msg | peak RSS | runs |")
        print("|---|---:|---:|---:|---:|")
        for c in order:
            k = (workload, codec, c)
            if k in rows:
                rs = rows[k]
                secs = statistics.median(r["secs"] for r in rs)
                cpu = statistics.median(r["cpu"] for r in rs)
                rss = statistics.median(r["rss_kb"] for r in rs)
                name = f"**{c}**" if c == "fastka" else c
                print(f"| {name} | {n / secs:,.0f} | {cpu / n * 1e6:.2f} | {rss / 1024:.0f} MB | {len(rs)} |")
            elif k in errors:
                print(f"| {c} | fails: {errors[k][:90]} | | | |")

lat = {c: rs for (w, _, c), rs in rows.items() if w == "latency"}
if lat:
    rate_n = next(iter(lat.values()))[0]["n"]
    print(f"\n**Produce → consume latency, {rate_n:,} messages at 5,000/s, one partition (fastka's producer for both; the consumer differs)**\n")
    print("| consumer | p50 | p99 | p99.9 | max |")
    print("|---|---:|---:|---:|---:|")
    for c in order:
        if c in lat:
            m = lambda k: statistics.median(r["latency_us"][k] for r in lat[c])
            name = f"**{c}**" if c == "fastka" else c
            print(f"| {name} | {m('p50'):,.0f} µs | {m('p99'):,.0f} µs | {m('p999'):,.0f} µs | {m('max'):,.0f} µs |")
