#!/usr/bin/env python3

from __future__ import annotations

import argparse
import heapq
import re
import sys
from dataclasses import dataclass
from pathlib import Path


_INT_RE = re.compile(r"^[0-9]+$")


@dataclass(frozen=True)
class LagEvent:
    lag_seconds: int
    metric_name: str


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Analyze a CarbonJ audit file and provide details about lagging metrics",
        epilog=(
            "Input format (whitespace-separated): <metric.name> <value> <timestamp>\n"
            "We use the first field as metric name and the last field as timestamp."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("audit_file", type=Path, help="Path to audit file")
    p.add_argument("topN", nargs="?", type=int, default=10, help="Max lag events to show (default: 10)")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    audit_file: Path = args.audit_file
    top_n: int = args.topN

    if top_n < 1:
        print(f"topN must be a positive integer, got: {top_n}", file=sys.stderr)
        return 2

    if not audit_file.is_file():
        print(f"File not found: {audit_file}", file=sys.stderr)
        return 2

    min_ts: int | None = None
    max_ts: int | None = None
    seen_max: int | None = None

    out_of_order_count = 0

    # Keep the top N lag events as a min-heap of (lag, seq, metric_name)
    # so we can efficiently maintain the largest lags.
    top_heap: list[tuple[int, int, str]] = []
    seq = 0

    with audit_file.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue

            metric = parts[0]
            ts_s = parts[-1]
            if not _INT_RE.match(ts_s):
                continue
            ts = int(ts_s)

            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts

            if seen_max is not None and ts < seen_max:
                lag = seen_max - ts
                out_of_order_count += 1
                seq += 1

                if len(top_heap) < top_n:
                    heapq.heappush(top_heap, (lag, seq, metric))
                else:
                    if lag > top_heap[0][0]:
                        heapq.heapreplace(top_heap, (lag, seq, metric))

            if seen_max is None or ts > seen_max:
                seen_max = ts

    if min_ts is None or max_ts is None:
        print("min_ts=NA")
        print("max_ts=NA")
        print("span_seconds=0")
        print("out_of_order=NO")
        return 0

    span = max_ts - min_ts
    print(f"min_ts={min_ts}")
    print(f"max_ts={max_ts}")
    print(f"span_seconds={span}")

    if out_of_order_count == 0:
        print("out_of_order=NO")
        return 0

    events = sorted(
        (LagEvent(lag, metric) for lag, _, metric in top_heap),
        key=lambda e: e.lag_seconds,
        reverse=True,
    )

    print("out_of_order=YES")
    print(f"out_of_order_count={out_of_order_count}")
    print(f"max_lag_seconds={events[0].lag_seconds}")
    print("")
    print(f"Top {len(events)} lag events (lag_seconds metric_name):")
    for i, e in enumerate(events, 1):
        print(f"{i:2d}) {e.lag_seconds}s {e.metric_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

