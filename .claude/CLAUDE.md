# CLAUDE.md — agent guide for `flink-stress-harness`

Onboarding for AI coding agents (and humans). Read this before editing.

## What this is

A highly configurable **synthetic-data Apache Flink 1.18 (Java 11) streaming
job** used to **A/B regression-test two Kubernetes clusters**: deploy the
identical workload to an old and a new cluster and compare throughput, latency,
resource use, backpressure, checkpointing and fault recovery — via Grafana
and/or an automated pass/fail scorecard.

The job is intentionally a *single configurable pipeline*; every behaviour is a
knob (see `config/HarnessConfig.java`).

## Pipeline (data flow)

```
SyntheticSource ─► [watermarks] ─► [fault:AFTER_SOURCE] ─► keyBy ─► window+aggregate ─► [fault:BEFORE_SINK] ─► sink
```

Wired in `HarnessJob.buildPipeline`. Watermarks are added only for **event-time**
time/session windows (skipped for `PROCESSING_TIME`); the strategy is selectable
(`watermark.strategy`). Faults can target stream boundaries (`AFTER_SOURCE`/
`BEFORE_SINK`, via `FaultInjectionMap`) or be embedded **inside** operators
(`SOURCE`/`AGGREGATE`/`SINK`, via `FaultInjector`); boundary stages are only
inserted when a fault targets them.

## Module map (`src/main/java/com/flinkstress/harness`)

| Package | Responsibility |
|---------|----------------|
| `HarnessJob` | `main`; builds env (state backend, checkpointing, restart) + pipeline |
| `config/` | `HarnessConfig` (all knobs, validation), enums, `ScenarioLoader` (YAML presets) |
| `model/` | `Event` (POJO, carries `emitTimeMs`/`eventTimeMs`), `AggResult` |
| `source/` | `RateController` (pure rate curve), `RecordFactory` (size/skew/late), `SyntheticSource` (token-bucket pacing) |
| `keyby/` | `HarnessKeySelector` (KEY_BY vs REBALANCE) |
| `window/` | `WindowFactory`, `WeightedAggregateFunction` (cpu-burn + state-heavy), `KeyStampWindowFunction` |
| `sink/` | `LatencyMeasuringSink` (Flink metrics), `SlidingHistogram` (self-contained), `ConsoleSink` |
| `fault/` | `FaultInjector` (shared pure scheduler used by the map + operators), `FaultInjectionMap` (boundary operator), `HaltStrategy`/`JvmHaltStrategy`, `FaultConfig` |

Scenario presets: `src/main/resources/scenarios/*.yaml`.

## Build / test / run (all via Docker — host JDK need not be 11)

```powershell
# Build + 68 unit tests + 12 MiniCluster integration tests + JaCoCo coverage
docker run --rm -v ${PWD}:/work -v maven-repo:/root/.m2 -w /work `
  maven:3.9-eclipse-temurin-11 mvn -B clean verify
# -> target/flink-stress-harness.jar (shaded), target/site/jacoco/index.html

# Local full-stack simulation (Flink + Prometheus + Grafana + MinIO)
docker compose up --build -d         # UI :8081, Prom :9090, Grafana :3001, MinIO :9001
docker compose down -v

# Helm (lint/render here; install on each target cluster)
docker run --rm -v ${PWD}:/apps -w /apps alpine/helm:3.14.4 lint helm/flink-stress-harness
docker run --rm -v ${PWD}:/apps -w /apps alpine/helm:3.14.4 template t helm/flink-stress-harness

# Scorecard (A/B verdict, exit non-zero on regression)
pip install -r scorecard/requirements.txt
pytest scorecard/test_compare.py -q
python scorecard/compare.py --baseline http://<A>:9090 --candidate http://<B>:9090
```

Test naming: `*Test.java` = unit (Surefire), `*ITCase.java` = MiniCluster
integration (Failsafe, `verify` phase).

## Conventions & design decisions (don't regress these)

- **Target Flink 1.18 exactly.** Use `open(Configuration)` in Rich functions —
  `open(OpenContext)` is 1.19+ and will not compile here.
- **Behavioural knobs (state backend, checkpoint, restart) flow as job ARGS**,
  not cluster `flinkConfiguration`. The job is the single source of truth; Helm
  only sets the Prometheus reporter + S3 filesystem creds in `flinkConfiguration`.
- **Testability seams are deliberate:** fault firing is a pure static
  `FaultInjector.evaluate(...)`; the TM-kill is behind `HaltStrategy` so
  tests use `RecordingHaltStrategy` instead of really halting; latency math is
  `LatencyMeasuringSink.latencyMs(...)`. Keep these pure/abstracted.
- **No dependency on Flink-internal classes** — `SlidingHistogram` implements
  the public `org.apache.flink.metrics.Histogram` rather than using internals.
- **Reproducible streams:** `RecordFactory` takes an injected seeded `Random`
  (`source.seed`) so both clusters process an identical stream.
- **Scenario precedence:** `--scenario X` loads the preset, then CLI `--key v`
  overrides individual keys (`HarnessConfig.fromArgs`).
- Latency = sink wall-clock − newest source `emitTimeMs` in the window; it
  *includes* window buffering by design (relative A/B signal, not an SLA).

## Gotchas (cost real debugging time — heed them)

- **Flink Prometheus reporter sanitizes label values** (spaces and `-` → `_`,
  operators get `Sink:`/`Source:` prefixes). Use **regex** selectors in PromQL/
  Grafana/scorecard, e.g. `operator_name=~".*latency.sink.*"`, never exact names.
- **Host JDK is newer than 11** — always build/test in the Maven JDK-11 container
  to match the Flink 1.18 runtime and avoid module-access errors.
- **Grafana** is mapped to host **:3001** in compose (:3000 was occupied).
- **The synthetic source is not replayable** (no checkpointed offsets); on a
  restart it regenerates from scratch, so a *record-count* fault re-fires every
  restart. This is correct for the continuous stress use-case; for a bounded
  recovery test use a one-shot (JVM-static) failure instead.

## Testing strategy

Three layers: pure unit tests (logic, statistics over seeded samples),
operator-harness tests (real operators, no cluster), and MiniCluster `*ITCase`
tests (full job, deterministic via bounded source + count-window=1 or
MAX_WATERMARK-on-completion). Job behaviours verified end-to-end include rate
pacing, late-data/watermarks, session grouping, skew→load concentration,
rebalance evenness, fault-fires, and restart recovery. ~86% instruction coverage;
the uncovered remainder is `Runtime.halt()` and POJO boilerplate.
