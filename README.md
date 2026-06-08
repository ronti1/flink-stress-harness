# Flink Stress Harness

A highly configurable, synthetic-data Apache Flink streaming job for **A/B
regression-testing two Kubernetes clusters**. Deploy the *identical* workload to
an old cluster and a new cluster, then compare throughput, latency, resource
efficiency, backpressure stability, checkpoint performance and fault-recovery —
either by eyeballing one Grafana, or via an automated pass/fail scorecard.

- **Engine:** Apache Flink 1.18 (DataStream API, Java 11), application mode via
  the Flink Kubernetes Operator 1.12.
- **Everything is a knob:** record size, firing rate/pattern, key skew, late
  data, window type, UDF weight, state backend, checkpointing, fault injection.
- **Batteries included:** Docker image, `docker compose` full-stack simulation,
  Helm chart (FlinkDeployment + Prometheus + Grafana + dashboard), Python
  comparison scorecard.

---

## Pipeline

```
SyntheticSource ─► [watermarks] ─► [fault:AFTER_SOURCE] ─► keyBy ─► window+aggregate ─► [fault:BEFORE_SINK] ─► sink
   rate modes        event-time       app-exc / TM-halt     skew /     tumbling-time |        app-exc / TM-halt     LatencyMeasuringSink
   size/skew/late    (time windows)                        rebalance   tumbling-count |                              (Flink metrics) | console
                                                                       session + UDF weight

Flink PrometheusReporter ─► Prometheus ─► Grafana dashboard
scorecard/compare.py ─► queries BOTH clusters' Prometheus ─► pass/fail + exit code
```

### Knobs (selected — see `HarnessConfig` for the full list)

| Area | Keys | Notes |
|------|------|-------|
| Source rate | `source.rateMode` = `CONSTANT\|RAMP\|MAX\|BURST`, `source.ratePerSec` | configured rate is job-wide, split across subtasks |
| Ramp | `source.ramp.{startRatePerSec,stepRatePerSec,stepSeconds,maxRatePerSec}` | find the throughput ceiling |
| Burst | `source.burst.{baseRatePerSec,peakRatePerSec,peakSeconds,periodSeconds}` | spike pattern |
| Record | `source.recordSizeBytes`, `source.numKeys`, `source.seed`, `source.maxRecords` | seed makes the A/B stream identical; `maxRecords>0` bounds the run |
| Skew | `skew.enabled`, `skew.hotKeyCount`, `skew.hotKeyTrafficPct` | hot-key % model |
| Partition | `partition.mode` = `KEY_BY\|REBALANCE` | skew lands on subtasks vs even spread |
| Late data | `late.enabled`, `late.pct`, `late.minMs`, `late.maxMs`, `watermark.boundedOutOfOrdernessMs`, `window.allowedLatenessMs` | random lateness in `[min,max]` for `pct` of records |
| Window | `window.type` = `TUMBLING_TIME\|TUMBLING_COUNT\|SESSION`, `window.sizeMs`, `window.countSize`, `window.sessionGapMs` | |
| UDF weight | `udf.cpuBurnMicros`, `udf.stateAccumulatorBytes` | independent CPU-burn and state-heavy knobs |
| Sink | `sink.mode` = `METRICS\|CONSOLE` | metrics sink registers the latency histogram |
| State | `state.backend` = `HASHMAP\|ROCKSDB`, `state.rocksdb.incremental` | |
| Checkpoint | `checkpoint.enabled`, `checkpoint.dir`, `checkpoint.intervalMs` (+ cluster `s3.*`) | S3-compatible (MinIO/Ceph/AWS) via uri + access/secret key |
| Restart | `restart.strategy` = `none\|fixed-delay\|exponential-delay`, `restart.attempts`, `restart.delayMs` | |
| Faults | `fault.app.*` (Java exception) and `fault.sys.*` (TM halt) | each: `stage` (`AFTER_SOURCE\|BEFORE_SINK`), `triggerMode` (`EVERY_N_RECORDS\|EVERY_MS`), `triggerN`, `maxOccurrences`, `probability` |

### Scenario presets

`--scenario NAME` loads `src/main/resources/scenarios/NAME.yaml` and CLI flags
override individual keys. Bundled: `ceiling-ramp`, `latency-soak`,
`kill-recovery`, `backpressure`, `skew-hotkey`, `late-data`.

### End-to-end latency definition

`e2eLatencyMs = sink wall-clock − newest source emit time among the records in
the window`. Window size therefore contributes to the measured latency; that is
intentional and consistent across clusters, so the A/B *comparison* stays valid.

---

## Build & test

The host JDK does not have to be 11 — everything builds and tests inside a
Maven JDK-11 container that matches the Flink 1.18 runtime.

```powershell
# Unit tests (51) + integration tests (2, on a Flink MiniCluster) + coverage
docker run --rm -v ${PWD}:/work -v maven-repo:/root/.m2 -w /work `
  maven:3.9-eclipse-temurin-11 mvn -B clean verify
```

Produces the shaded job jar at `target/flink-stress-harness.jar` and a JaCoCo
report at `target/site/jacoco/index.html`.

Scorecard tests:

```powershell
pip install -r scorecard/requirements.txt
pytest scorecard/test_compare.py -q
```

---

## Local full-stack simulation (docker compose)

Spins up a Flink application cluster running the job, plus Prometheus, Grafana
(dashboard pre-provisioned) and MinIO (for checkpoint-to-S3 scenarios).

```powershell
docker compose up --build -d
```

| Service | URL |
|---------|-----|
| Flink UI | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana (dashboard "Flink Stress Harness") | http://localhost:3001 — admin/admin |
| MinIO console | http://localhost:9001 — minioadmin/minioadmin |

Change the workload by editing the `jobmanager.command` in `docker-compose.yml`
(e.g. `--scenario ceiling-ramp` or `--scenario backpressure`). Tear down with
`docker compose down -v`.

---

## Kubernetes A/B deployment (Helm)

The Flink Kubernetes Operator is assumed to be installed cluster-wide; this
chart ships only the `FlinkDeployment` CR plus a self-contained Prometheus +
Grafana with the dashboard provisioned.

```powershell
# Push the image to a registry both clusters can pull, then on EACH cluster:
helm install harness helm/flink-stress-harness `
  --set image.repository=<registry>/flink-stress-harness --set image.tag=<tag> `
  --set harness.scenario=latency-soak `
  --set state.backend=rocksdb `
  --set checkpoint.enabled=true --set checkpoint.dir=s3://flink-checkpoints/harness `
  --set s3.endpoint=http://minio.minio.svc:9000 --set s3.accessKey=... --set s3.secretKey=...
```

Use **identical values on both clusters** so the only variable is the cluster
itself. `helm/flink-stress-harness/values.yaml` documents every setting.

---

## Scorecard (automated A/B verdict)

```powershell
python scorecard/compare.py `
  --baseline  http://<clusterA-prometheus>:9090 `
  --candidate http://<clusterB-prometheus>:9090 `
  --tolerances scorecard/tolerances.yaml --window 10m
```

Prints a per-metric table and exits non-zero on any regression (pipeline-gate
friendly). Metrics, aggregation and tolerances are declared in
`scorecard/tolerances.yaml`.

---

## Metrics reference (Prometheus)

| Signal | PromQL |
|--------|--------|
| Source throughput | `sum(flink_taskmanager_job_task_numRecordsOutPerSecond{task_name=~".*synthetic.source.*"})` |
| End-to-end throughput | `sum(flink_taskmanager_job_task_operator_processedRecordsPerSec{operator_name=~".*latency.sink.*"})` |
| E2E latency p99 | `max(flink_taskmanager_job_task_operator_e2eLatencyMs{operator_name=~".*latency.sink.*",quantile="0.99"})` |
| Backpressure / busy | `flink_taskmanager_job_task_backPressuredTimeMsPerSecond`, `..._busyTimeMsPerSecond` |
| CPU / heap | `flink_taskmanager_Status_JVM_CPU_Load`, `flink_taskmanager_Status_JVM_Memory_Heap_Used` |
| Checkpoints | `flink_jobmanager_job_lastCheckpointDuration`, `..._numberOfCompletedCheckpoints` |
| Recovery (restarts) | `flink_jobmanager_job_numRestarts` |

> Note: the Flink Prometheus reporter sanitizes label values (spaces and `-`
> become `_`, and operators get a `Sink:`/`Source:` prefix), so selectors use
> regex matches rather than exact names.

---

## Project layout

```
pom.xml                     Maven build (shaded fat jar, surefire+failsafe, jacoco)
Dockerfile                  Multi-stage: build jar -> Flink 1.18 runtime + plugins
docker-compose.yml          Full local simulation stack
src/main/java/com/flinkstress/harness/
  HarnessJob.java           Wires the pipeline from config
  config/                   HarnessConfig, enums, ScenarioLoader
  model/                    Event, AggResult
  source/                   RateController, RecordFactory, SyntheticSource
  keyby/                    HarnessKeySelector
  window/                   WindowFactory, WeightedAggregateFunction, KeyStampWindowFunction
  sink/                     LatencyMeasuringSink, SlidingHistogram, ConsoleSink
  fault/                    FaultInjectionMap, HaltStrategy, FaultConfig
src/main/resources/scenarios/  Scenario presets
src/test/java/...           51 unit tests + MiniCluster ITCase
helm/flink-stress-harness/  Chart: FlinkDeployment + Prometheus + Grafana
dashboards/                 Grafana dashboard JSON
scorecard/                  compare.py + tests + tolerances.yaml
```

## Requirements

- Docker (for build, tests, local simulation, helm).
- A Flink Kubernetes Operator 1.12 install on each target cluster (for Helm deploy).
- Python 3.9+ for the scorecard.
