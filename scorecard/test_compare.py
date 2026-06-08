"""Unit tests for the scorecard comparison logic (no network)."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
import compare  # noqa: E402


def test_aggregate_modes():
    vals = [1, 2, 3, 4, 100]
    assert compare.aggregate(vals, "max") == 100
    assert compare.aggregate(vals, "min") == 1
    assert compare.aggregate(vals, "avg") == 22
    assert compare.aggregate([], "avg") is None


def test_aggregate_percentile_nearest_rank():
    vals = list(range(1, 101))  # 1..100
    assert compare.aggregate(vals, "p50") == 50
    assert compare.aggregate(vals, "p99") == 99
    assert compare.aggregate(vals, "p100") == 100


def test_parse_query_range_flattens_and_skips_nan():
    payload = {
        "data": {
            "result": [
                {"metric": {}, "values": [[0, "1.5"], [15, "2.5"]]},
                {"metric": {}, "values": [[0, "NaN"], [15, "3.0"]]},
            ]
        }
    }
    assert compare.parse_query_range(payload) == [1.5, 2.5, 3.0]


def test_evaluate_higher_is_better_pass_and_fail():
    # candidate within 10% below baseline -> pass
    r = compare.evaluate("thr", 1000.0, 950.0, "higher_is_better", 10)
    assert r["status"] == "pass"
    # candidate 20% below baseline -> fail
    r = compare.evaluate("thr", 1000.0, 800.0, "higher_is_better", 10)
    assert r["status"] == "fail"


def test_evaluate_lower_is_better_pass_and_fail():
    # candidate 10% above baseline, tol 15 -> pass
    r = compare.evaluate("lat", 100.0, 110.0, "lower_is_better", 15)
    assert r["status"] == "pass"
    # candidate 30% above baseline, tol 15 -> fail
    r = compare.evaluate("lat", 100.0, 130.0, "lower_is_better", 15)
    assert r["status"] == "fail"


def test_evaluate_floor_guards_noise():
    # both tiny -> pass regardless of ratio
    r = compare.evaluate("bp", 5.0, 40.0, "lower_is_better", 10, floor=50)
    assert r["status"] == "pass"


def test_evaluate_missing_data():
    assert compare.evaluate("x", None, None, "lower_is_better", 10)["status"] == "skipped"
    assert compare.evaluate("x", 1.0, None, "lower_is_better", 10)["status"] == "fail"


def test_compare_uses_fetchers_and_specs():
    specs = [
        {"name": "thr", "promql": "q_thr", "aggregate": "max",
         "direction": "higher_is_better", "tolerance_pct": 10},
        {"name": "lat", "promql": "q_lat", "aggregate": "max",
         "direction": "lower_is_better", "tolerance_pct": 10},
    ]
    baseline = {"q_thr": [1000.0], "q_lat": [100.0]}
    candidate = {"q_thr": [980.0], "q_lat": [105.0]}

    results = compare.compare(specs, lambda q: baseline[q], lambda q: candidate[q])
    by_name = {r["metric"]: r for r in results}
    assert by_name["thr"]["status"] == "pass"
    assert by_name["lat"]["status"] == "pass"


def test_duration_parsing():
    assert compare._duration_s("15s") == 15
    assert compare._duration_s("10m") == 600
    assert compare._duration_s("1h") == 3600
    assert compare._duration_s(45) == 45


def test_render_smoke():
    results = [compare.evaluate("thr", 1000.0, 950.0, "higher_is_better", 10)]
    text = compare.render(results)
    assert "METRIC" in text and "THR".lower() in text.lower()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
