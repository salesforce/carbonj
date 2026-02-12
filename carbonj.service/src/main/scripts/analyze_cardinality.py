#!/usr/bin/env python3
"""
Parallel high-cardinality metric analyzer for CarbonJ audit files.

CarbonJ periodically dumps all known metric names into an audit file where each
line has the format: <metric_name> <value> <timestamp>.  Metric names are
dot-separated hierarchical paths (e.g.
pod217.ecom.bgzd.bgzd_prd.blade9-4.bgzd_prd.ocapi.scapi.responses.custom.data.v1.customers_123).

This script reads the audit file using all available CPU cores and identifies
metric name patterns with high cardinality -- i.e. prefixes that fan out into a
large number of unique child segments.  High cardinality is a common cause of
excessive memory and storage usage in time-series databases.

The output includes:
  - Top prefixes ranked by number of unique direct children
  - Metric families ranked by total unique series
  - Per-position cardinality analysis across the dot-separated path
  - Drill-down into which parent prefixes contribute the most unique values

Usage:
    python3 analyze_cardinality.py [path_to_audit_file]

If no path is provided, defaults to audit.txt in the current directory.
"""

import os
import sys
import multiprocessing as mp
from collections import Counter, defaultdict
import time


def find_chunk_boundaries(filename, num_chunks):
    """Find line-aligned chunk boundaries for parallel processing."""
    file_size = os.path.getsize(filename)
    boundaries = []
    chunk_size = file_size // num_chunks

    with open(filename, 'rb') as f:
        start = 0
        for i in range(num_chunks - 1):
            target = start + chunk_size
            if target >= file_size:
                break
            f.seek(target)
            f.readline()  # align to next line boundary
            end = f.tell()
            boundaries.append((start, end))
            start = end
        boundaries.append((start, file_size))

    return boundaries


def process_chunk(args):
    """Process a chunk of the file and return metric name counts and prefix analysis."""
    filename, start, end, chunk_id = args

    metric_names = set()
    prefix_children = defaultdict(set)  # prefix -> set of next-level values

    with open(filename, 'rb') as f:
        f.seek(start)
        bytes_to_read = end - start
        data = f.read(bytes_to_read)

    for line in data.split(b'\n'):
        if not line:
            continue
        space_idx = line.find(b' ')
        if space_idx == -1:
            continue
        metric = line[:space_idx].decode('utf-8', errors='replace')
        metric_names.add(metric)

        parts = metric.split('.')
        for depth in range(1, len(parts)):
            prefix = '.'.join(parts[:depth])
            child = parts[depth]
            prefix_children[prefix].add(child)

    return metric_names, dict(prefix_children)


def merge_prefix_children(all_results):
    """Merge prefix->children sets from all chunks."""
    merged = defaultdict(set)
    for _, prefix_children in all_results:
        for prefix, children in prefix_children.items():
            merged[prefix].update(children)
    return merged


def main():
    audit_file = sys.argv[1] if len(sys.argv) > 1 else "audit.txt"
    if not os.path.exists(audit_file):
        print(f"Error: File not found: {audit_file}")
        sys.exit(1)

    num_cores = mp.cpu_count()
    start_time = time.time()
    file_size_mb = os.path.getsize(audit_file) / (1024 * 1024)
    print(f"Analyzing: {audit_file}")
    print(f"File size: {file_size_mb:.1f} MB")
    print(f"Using {num_cores} CPU cores")
    print(f"{'='*80}")

    # Split file into chunks
    boundaries = find_chunk_boundaries(audit_file, num_cores)
    chunk_args = [
        (audit_file, start, end, i)
        for i, (start, end) in enumerate(boundaries)
    ]

    print(f"Split into {len(chunk_args)} chunks, processing in parallel...")

    # Process in parallel
    with mp.Pool(num_cores) as pool:
        results = pool.map(process_chunk, chunk_args)

    parse_time = time.time()
    print(f"Parsing completed in {parse_time - start_time:.1f}s")

    # Merge all unique metric names
    all_metrics = set()
    for metric_names, _ in results:
        all_metrics.update(metric_names)

    print(f"\nTotal unique metric names: {len(all_metrics):,}")

    # Merge prefix children
    print("Merging prefix analysis...")
    merged_prefixes = merge_prefix_children(results)

    # ── Top high cardinality prefixes ────────────────────────────────────
    print(f"\n{'='*80}")
    print("TOP 50 HIGH CARDINALITY PREFIXES")
    print("(prefix -> number of unique direct children at the next dot-level)")
    print(f"{'='*80}\n")

    sorted_prefixes = sorted(
        merged_prefixes.items(),
        key=lambda x: len(x[1]),
        reverse=True
    )

    for i, (prefix, children) in enumerate(sorted_prefixes[:50]):
        depth = prefix.count('.') + 1
        card = len(children)
        sample = sorted(children)[:5]
        sample_str = ', '.join(sample)
        if len(children) > 5:
            sample_str += f', ... (+{len(children)-5} more)'
        print(f"  {i+1:3d}. [{card:,} unique children] (depth {depth})")
        print(f"       Prefix: {prefix}")
        print(f"       Sample: {sample_str}")
        print()

    # ── Metric families by total unique series ───────────────────────────
    print(f"\n{'='*80}")
    print("TOP 30 METRIC FAMILIES BY TOTAL UNIQUE SERIES")
    print("(grouping by first N dot-segments, counting total unique full metric names)")
    print(f"{'='*80}\n")

    for depth in [3, 4, 5, 6]:
        family_counts = Counter()
        for metric in all_metrics:
            parts = metric.split('.')
            if len(parts) >= depth:
                family = '.'.join(parts[:depth])
                family_counts[family] += 1

        print(f"--- At depth {depth} (first {depth} segments) ---")
        for family, count in family_counts.most_common(15):
            print(f"  {count:>8,} series  |  {family}")
        print()

    # ── Cardinality explosion by position ────────────────────────────────
    print(f"\n{'='*80}")
    print("CARDINALITY EXPLOSION ANALYSIS")
    print("Finding which position in the metric path has an explosion of unique values")
    print(f"{'='*80}\n")

    depth_values = defaultdict(set)
    max_depth = 0
    for metric in all_metrics:
        parts = metric.split('.')
        max_depth = max(max_depth, len(parts))
        for i, part in enumerate(parts):
            depth_values[i].add(part)

    print(f"Max metric depth: {max_depth} segments\n")
    print(f"  {'Position':>10} | {'Unique Values':>15} | {'Sample Values'}")
    print(f"  {'-'*10}-+-{'-'*15}-+-{'-'*50}")
    for i in range(max_depth):
        vals = depth_values[i]
        count = len(vals)
        sample = sorted(vals)[:5]
        sample_str = ', '.join(sample)
        if count > 5:
            sample_str += ' ...'
        flag = " <<<< HIGH CARDINALITY" if count > 500 else ""
        print(f"  {i:>10} | {count:>15,} | {sample_str}{flag}")

    # ── Drill-down on high cardinality positions ─────────────────────────
    print(f"\n{'='*80}")
    print("DRILL-DOWN: High cardinality positions broken down by parent prefix")
    print(f"{'='*80}\n")

    high_card_positions = [i for i in range(max_depth) if len(depth_values[i]) > 500]
    for pos in high_card_positions:
        print(f"Position {pos} has {len(depth_values[pos]):,} unique values")
        parent_child = defaultdict(set)
        for metric in all_metrics:
            parts = metric.split('.')
            if len(parts) > pos:
                parent = '.'.join(parts[:pos]) if pos > 0 else '<root>'
                parent_child[parent].add(parts[pos])

        sorted_parents = sorted(parent_child.items(), key=lambda x: len(x[1]), reverse=True)
        print(f"  Top parent prefixes contributing unique values at position {pos}:")
        for parent, children in sorted_parents[:20]:
            sample = sorted(children)[:5]
            sample_str = ', '.join(sample)
            if len(children) > 5:
                sample_str += ' ...'
            print(f"    [{len(children):>6,} unique] {parent}")
            print(f"                      Samples: {sample_str}")
        print()

    elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"Analysis completed in {elapsed:.1f}s using {num_cores} cores")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
