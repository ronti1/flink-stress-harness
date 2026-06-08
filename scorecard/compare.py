#!/usr/bin/env python3
"""A/B regression scorecard for the Flink stress harness.

Queries the Prometheus of a *baseline* cluster and a *candidate* cluster over a
time window, aggregates a set of signal metrics, and asserts that the candidate
is within tolerance of the baseline. Prints a table and exits non-zero on any
regression so it can gate a pipeline.

The HTTP layer and the comparison logic are decoupled: ``compare()`` takes fetch
callables, so the decision logic is unit tested without a live Prometheus.
"""
import argparse
import json
import math
import sys
import time
from statistics import mean
from typing import Callable, Dict, List, Optional

import requests
import yaml

DEFAULT_SPECS = [
    {
        "name": "throughput_processed_per_sec",
        "promql": 'sum(flink_taskmanager_job_task_operator_processedRecordsPerSec{operator_name=~".*latency.sink.*"})',
        "aggregate": "p95",
        "direction": "higher_is_better",
        "tolerance_pct": 10,
    },
    {
        "name": "latency_p99_ms",
        "promql": 'max(flink_taskmanager_job_task_operator_e2eLatencyMs{operator_name=~".*latency.sink.*",quantile="0.99"})',
        "aggregate": "p95",
        "direction": "lower_is_better",
        "tolerance_pct": 15,
    },
    {
        "name": "tm_cpu_load_pct",
        "promql": "100 * avg(flink_taskmanager_Status_JVM_CPU_Load)",
        "aggregate": "avg",
        "direction": "lower_is_better",
        "tolerance_pct": 20,
    },
    {
        "name": "backpressure_ms_per_sec",
        "promql": "max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)",
        "aggregate": "p95",
        "direction": "lower_is_better",
        "tolerance_pct": 25,
        "absolute_floor": 50,
    },
]


def aggregate(values: List[float], how: str) -> Optional[float]:
    """Reduces a series of samples to a single number."""
    if not values:
        return None
    if how == "max":
        return max(values)
    if how == "min":
        return min(values)
    if how == "avg":
        return mean(values)
    if how.startswith("p"):
        q = float(how[1:]) / 100.0
        ordered = sorted(values)
        # nearest-rank percentile (matches the Java SlidingHistogram)
        rank = max(1, min(len(ordered), math.ceil(q * len(ordered))))
        return ordered[rank - 1]
    raise ValueError(f"unknown aggregate: {how}")


def parse_query_range(payload: dict) -> List[float]:
    """Flattens a Prometheus query_range matrix response into a list of floats."""
    out: List[float] = []
    result = payload.get("data", {}).get("result", [])
    for series in result:
        for _ts, raw in series.get("values", []):
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            if v != v:  # NaN
                continue
            out.append(v)
    return out


def evaluate(name: str, baseline: Optional[float], candidate: Optional[float],
             direction: str, tolerance_pct: float, floor: float = 0.0) -> Dict:
    """Compares one metric; returns a result record with a status."""
    result = {
        "metric": name,
        "baseline": baseline,
        "candidate": candidate,
        "direction": direction,
        "tolerance_pct": tolerance_pct,
    }
    if baseline is None and candidate is None:
        result["status"] = "skipped"
        result["detail"] = "no data on either side"
        return result
    if baseline is None or candidate is None:
        result["status"] = "fail"
        result["detail"] = "metric present on only one cluster"
        return result

    # Treat sub-floor magnitudes as negligible to avoid noise-driven failures.
    if abs(baseline) < floor and abs(candidate) < floor:
        result["status"] = "pass"
        result["detail"] = f"both below floor ({floor})"
        result["delta_pct"] = 0.0
        return result

    if direction == "higher_is_better":
        threshold = baseline * (1 - tolerance_pct / 100.0)
        ok = candidate >= threshold
    elif direction == "lower_is_better":
        threshold = baseline * (1 + tolerance_pct / 100.0)
        ok = candidate <= threshold
    else:
        raise ValueError(f"unknown direction: {direction}")

    result["delta_pct"] = ((candidate - baseline) / baseline * 100.0) if baseline else None
    result["threshold"] = threshold
    result["status"] = "pass" if ok else "fail"
    return result


def compare(specs: List[dict],
            baseline_fetch: Callable[[str], List[float]],
            candidate_fetch: Callable[[str], List[float]]) -> List[Dict]:
    """Runs every metric spec through both fetchers and evaluates each."""
    results = []
    for spec in specs:
        how = spec.get("aggregate", "avg")
        b = aggregate(baseline_fetch(spec["promql"]), how)
        c = aggregate(candidate_fetch(spec["promql"]), how)
        results.append(evaluate(
            spec["name"], b, c,
            spec.get("direction", "lower_is_better"),
            float(spec.get("tolerance_pct", 10)),
            float(spec.get("absolute_floor", 0.0)),
        ))
    return results


class PromClient:
    """Thin Prometheus HTTP API client."""

    def __init__(self, base_url: str, window_s: int, step_s: int, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.window_s = window_s
        self.step_s = step_s
        self.timeout = timeout

    def query_range(self, promql: str) -> List[float]:
        end = time.time()
        start = end - self.window_s
        resp = requests.get(
            f"{self.base_url}/api/v1/query_range",
            params={"query": promql, "start": start, "end": end, "step": self.step_s},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return parse_query_range(resp.json())


def render(results: List[Dict]) -> str:
    lines = []
    header = f"{'METRIC':<32} {'BASELINE':>14} {'CANDIDATE':>14} {'DELTA%':>9} {'STATUS':>8}"
    lines.append(header)
    lines.append("-" * len(header))
    for r in results:
        b = "-" if r["baseline"] is None else f"{r['baseline']:.3f}"
        c = "-" if r["candidate"] is None else f"{r['candidate']:.3f}"
        d = "-" if r.get("delta_pct") is None else f"{r['delta_pct']:+.1f}"
        lines.append(f"{r['metric']:<32} {b:>14} {c:>14} {d:>9} {r['status'].upper():>8}")
    return "\n".join(lines)


def load_specs(path: Optional[str]) -> (List[dict], dict):
    if not path:
        return DEFAULT_SPECS, {}
    with open(path) as fh:
        doc = yaml.safe_load(fh) or {}
    return doc.get("metrics", DEFAULT_SPECS), doc


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="A/B regression scorecard for the Flink stress harness")
    p.add_argument("--baseline", required=True, help="Baseline cluster Prometheus base URL")
    p.add_argument("--candidate", required=True, help="Candidate cluster Prometheus base URL")
    p.add_argument("--tolerances", help="YAML file with metric specs (defaults built-in)")
    p.add_argument("--window", default="10m", help="Look-back window, e.g. 10m, 1h")
    p.add_argument("--step", default="15s", help="Query resolution step, e.g. 15s")
    p.add_argument("--output", default="scorecard-report.json", help="Where to write the JSON report")
    args = p.parse_args(argv)

    specs, doc = load_specs(args.tolerances)
    window_s = _duration_s(doc.get("window", args.window))
    step_s = _duration_s(doc.get("step", args.step))

    baseline = PromClient(args.baseline, window_s, step_s)
    candidate = PromClient(args.candidate, window_s, step_s)

    results = compare(specs, baseline.query_range, candidate.query_range)
    failed = [r for r in results if r["status"] == "fail"]
    overall = "FAIL" if failed else "PASS"

    print(render(results))
    print(f"\nOVERALL: {overall}  ({len(failed)} regression(s) of {len(results)} metrics)")

    report = {"overall": overall, "window_s": window_s, "results": results}
    with open(args.output, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"Report written to {args.output}")

    return 1 if failed else 0


def _duration_s(text) -> int:
    """Parses a Prometheus-style duration (s/m/h) or a bare number of seconds."""
    if isinstance(text, (int, float)):
        return int(text)
    text = str(text).strip()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(float(text))


if __name__ == "__main__":
    sys.exit(main())
