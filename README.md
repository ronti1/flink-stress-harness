# Flink Stress Harness

A configurable, synthetic-data **Apache Flink** streaming job whose single purpose is to
**prove that a new Kubernetes cluster behaves the same as an old one** before you migrate
real workloads onto it.

You run the *exact same* job on both clusters, push the metrics into Grafana, and either
eyeball the two side-by-side or let the included Python **scorecard** give you a
machine-readable **PASS/FAIL**. If the new cluster is slower, uses more CPU, drops
throughput, or recovers more slowly from failures, you find out *here* — not in production.

> **New to Flink?** Read the [Flink in 5 minutes](#flink-in-5-minutes-read-this-first)
> primer first. Every Flink term used in this README is defined there.

---

## Table of contents

1. [What you get](#what-you-get)
2. [Quick start (10 minutes, no Kubernetes)](#quick-start-10-minutes-no-kubernetes)
3. [Flink in 5 minutes (read this first)](#flink-in-5-minutes-read-this-first)
4. [How the harness is built (the pipeline)](#how-the-harness-is-built-the-pipeline)
5. [Configuration reference (every knob)](#configuration-reference-every-knob)
6. [Fault injection explained](#fault-injection-explained)
7. [Time & watermarks explained](#time--watermarks-explained)
8. [Scenario presets](#scenario-presets)
9. [Build & test](#build--test)
10. [Local full-stack simulation (Docker Compose)](#local-full-stack-simulation-docker-compose)
11. [Reading the Grafana dashboard](#reading-the-grafana-dashboard)
12. [Deploying to Kubernetes (Helm)](#deploying-to-kubernetes-helm)
13. [The A/B scorecard](#the-ab-scorecard)
14. [Metrics reference (PromQL)](#metrics-reference-promql)
15. [Project layout](#project-layout)
16. [Extending the harness](#extending-the-harness)
17. [Troubleshooting & FAQ](#troubleshooting--faq)
18. [What is actually verified](#what-is-actually-verified)
19. [Requirements](#requirements)

---

## What you get

- **Engine:** Apache Flink **1.18** (DataStream API, **Java 11**), running in **application
  mode** via the Flink **Kubernetes Operator 1.12**.
- **Everything is a knob:** firing rate & pattern, record size, key skew, late data,
  event vs processing time, watermark strategy, window type, UDF (compute/state) weight,
  state backend, checkpointing, restart strategy, and **fault injection at five points in
  the pipeline**. See the [full reference](#configuration-reference-every-knob).
- **Batteries included:**
  - a multi-stage **Dockerfile** that builds the job and layers it on the Flink runtime;
  - a **Docker Compose** stack that runs Flink + Prometheus + Grafana + MinIO locally so
    you can see the whole thing work on your laptop with no Kubernetes;
  - a **Helm chart** that deploys the job (as a `FlinkDeployment`) plus a self-contained
    Prometheus + Grafana with the dashboard pre-loaded;
  - a **Python scorecard** that compares two clusters and exits non-zero on regression
    (drop it straight into a CI gate).
- **80 automated tests** (68 unit + 12 Flink MiniCluster integration tests).

---

## Quick start (10 minutes, no Kubernetes)

You only need **Docker Desktop**. This brings up Flink running the job, Prometheus
scraping it, Grafana showing it, and MinIO as S3-compatible checkpoint storage.

```powershell
git clone https://github.com/ronti1/flink-stress-harness.git
cd flink-stress-harness

docker compose up --build -d        # first build ~2–3 min (downloads Flink deps)
```

Then open:

| Service | URL | Notes |
|---|---|---|
| **Flink UI** | http://localhost:8081 | the running job, its operators, backpressure |
| **Grafana** | http://localhost:3001 | dashboard **"Flink Stress Harness"** (anonymous admin) |
| **Prometheus** | http://localhost:9090 | raw metric queries |
| **MinIO console** | http://localhost:9001 | `minioadmin` / `minioadmin` |

Within ~30 seconds the Grafana dashboard shows live throughput and latency.

To run a **different workload**, edit the `jobmanager.command` in `docker-compose.yml`
(e.g. change `--scenario latency-soak` to `--scenario ceiling-ramp`) and run
`docker compose up -d --force-recreate jobmanager taskmanager`.

Tear everything down (including volumes) with:

```powershell
docker compose down -v
```

---

## Flink in 5 minutes (read this first)

You do **not** need to be a Flink expert to operate or extend this project, but you need
these terms. Each maps to something concrete in this repo.

| Flink term | What it means | Where it shows up here |
|---|---|---|
| **Job / pipeline** | A directed graph of operators that processes an unbounded stream. | The whole harness; built in `HarnessJob.buildPipeline`. |
| **JobManager (JM)** | The "master" process. Schedules work, coordinates checkpoints, talks to Kubernetes. | One pod per job (application mode). |
| **TaskManager (TM)** | A "worker" process that actually runs operator code in **task slots**. | Scaled by `taskManager.replicas` / slots. Killing one is the **system fault**. |
| **Operator** | One step in the pipeline (source, map, window, sink). | `SyntheticSource`, `WindowFactory`, `LatencyMeasuringSink`, etc. |
| **Parallelism / subtask** | An operator runs as N parallel **subtasks**, one per slot. | `job.parallelism`. Each source subtask makes its share of the rate. |
| **`keyBy`** | Partitions the stream by a key so all records with the same key go to the same subtask (needed for keyed state & per-key windows). | `HarnessKeySelector`. |
| **Window** | Groups records over time or count so you can aggregate them. | `WindowFactory` (tumbling-time, tumbling-count, session). |
| **Event time vs processing time** | *Event time* = when the record "happened" (a timestamp in the data). *Processing time* = the wall-clock when Flink sees it. | `time.characteristic`. |
| **Watermark** | A marker that says "event time has advanced to T; no records older than T should still arrive". Drives when event-time windows fire. | `watermark.strategy`, `watermark.boundedOutOfOrdernessMs`. |
| **Late data** | A record whose event time is older than the current watermark. | Injected by `late.*` knobs. |
| **State & state backend** | Operators remember things (e.g. a window's running total). The **state backend** decides where: heap (`HASHMAP`) or on-disk RocksDB. | `state.backend`. |
| **Checkpoint** | A periodic, consistent snapshot of all state to durable storage (here: S3/MinIO). On failure Flink restores from the last checkpoint. | `checkpoint.*`. |
| **Backpressure** | When a downstream operator can't keep up, it slows upstream operators. The classic "the pipeline is overloaded" signal. | Dashboard panel; the `backpressure` scenario. |
| **Restart strategy** | What Flink does when an operator throws: how many times and how fast to restart. | `restart.*`. |
| **Operator chaining** | Flink fuses adjacent operators into one task (same thread) for speed. | Why an in-operator halt kills the expected TM. |

If you remember one sentence: **a Flink job is a graph of operators; the JobManager
schedules them onto TaskManager slots; windows group records; checkpoints make it
fault-tolerant.** Everything below is a knob on that machine.

---

## How the harness is built (the pipeline)

The job is a single, linear pipeline. **Fault injection can happen at five points** — three
*inside* real operators (`SOURCE`, `AGGREGATE`, `SINK`) and two as dedicated *pass-through*
operators at the stream boundaries (`AFTER_SOURCE`, `BEFORE_SINK`). A fault point only
appears in the graph if you target it.

```
                         ┌────────────── optional fault points ──────────────┐
                         │  SOURCE        AFTER_SOURCE     AGGREGATE   BEFORE_SINK   SINK
                         ▼  (in-op)       (boundary map)   (in-op)    (boundary map)(in-op)
 ┌───────────────┐   ┌──────────┐   ┌────────┐   ┌───────────────────┐   ┌────────┐   ┌────────────────────┐
 │ SyntheticSource│─►│watermarks │─►│ keyBy   │─►│ window + aggregate │─►│ (map)  │─►│ sink               │
 │ rate/size/skew │  │(event-time│  │ skew /  │  │ tumbling-time |     │  │        │  │ LatencyMeasuringSink│
 │ /late/seed     │  │ only)     │  │ rebalance│ │ tumbling-count |    │  │        │  │ (Flink metrics)     │
 └───────────────┘   └──────────┘   └────────┘   │ session + UDF weight│  └────────┘   │   — or — ConsoleSink│
                                                 └───────────────────┘               └────────────────────┘

   Flink PrometheusReporter ─► Prometheus ─► Grafana dashboard
   scorecard/compare.py ─────► queries BOTH clusters' Prometheus ─► PASS/FAIL + exit code
```

Stage by stage:

1. **`SyntheticSource`** generates `Event` records at the configured rate/pattern, with the
   configured size, key-skew, and late-data injection. A **seed** makes the stream
   byte-identical on both clusters (so any difference is the cluster, not the data).
2. **watermarks** are assigned *only* for **event-time** time/session windows (skipped
   entirely for processing-time or count windows).
3. **`keyBy`** partitions by key (`KEY_BY`) so skew lands on specific subtasks, or
   round-robins (`REBALANCE`) for an even spread.
4. **window + aggregate** groups records and runs the **`WeightedAggregateFunction`**,
   whose CPU cost and per-key state size are independent knobs.
5. **sink** either turns the output into Flink **metrics** (throughput + an end-to-end
   latency histogram) or just logs to the console.

The **latency** the sink reports is defined as:

```
e2eLatencyMs = (wall-clock at the sink)  −  (newest source emit-time among the
                                             records that formed this window)
```

This *includes* the time records sat buffered in the window, by design. It is a
**relative A/B signal**, not an SLA — identical on both clusters, so comparing the two is
valid.

---

## Configuration reference (every knob)

The job is configured entirely by `--key value` command-line args (and/or a
[`--scenario`](#scenario-presets) preset). Every knob, its default, and its meaning:

### Job
| Key | Default | Meaning |
|---|---|---|
| `job.name` | `flink-stress-harness` | Flink job name. |
| `job.parallelism` | `1` | Default parallelism for all operators. |
| `source.parallelism` | `0` | Source-only parallelism; `0` = use `job.parallelism`. |

### Source — rate
| Key | Default | Meaning |
|---|---|---|
| `source.rateMode` | `CONSTANT` | `CONSTANT` (steady), `RAMP` (step up to find the ceiling), `MAX` (as fast as possible), `BURST` (base/peak cycles). |
| `source.ratePerSec` | `10000` | Target **job-wide** records/sec for `CONSTANT` (split across subtasks). |
| `source.recordSizeBytes` | `256` | Payload bytes per record (controls bytes/sec). |
| `source.numKeys` | `1000` | Size of the key space. |
| `source.maxRecords` | `0` | Stop after N records job-wide; `0` = run forever. |
| `source.seed` | `1234` | RNG seed; **keep identical on both clusters** for a fair A/B. |

### Source — ramp (used when `rateMode=RAMP`)
| Key | Default | Meaning |
|---|---|---|
| `source.ramp.startRatePerSec` | `1000` | Starting rate. |
| `source.ramp.stepRatePerSec` | `1000` | Rate added each step. |
| `source.ramp.stepSeconds` | `10` | Seconds between steps. |
| `source.ramp.maxRatePerSec` | `0` | Ceiling; `0` = unbounded ramp. |

### Source — burst (used when `rateMode=BURST`)
| Key | Default | Meaning |
|---|---|---|
| `source.burst.baseRatePerSec` | `1000` | Rate between spikes. |
| `source.burst.peakRatePerSec` | `20000` | Rate during a spike. |
| `source.burst.peakSeconds` | `5` | Spike duration. |
| `source.burst.periodSeconds` | `30` | Spike period (spike + quiet). |

### Key skew & partitioning
| Key | Default | Meaning |
|---|---|---|
| `skew.enabled` | `false` | Turn on hot-key skew. |
| `skew.hotKeyCount` | `10` | Number of hot keys. |
| `skew.hotKeyTrafficPct` | `0.8` | Fraction of traffic sent to the hot keys (0–1). |
| `partition.mode` | `KEY_BY` | `KEY_BY` (skew concentrates on subtasks) or `REBALANCE` (even round-robin). |

### Late data & time
| Key | Default | Meaning |
|---|---|---|
| `late.enabled` | `false` | Inject late records. |
| `late.pct` | `0.05` | Fraction of records made late (0–1). |
| `late.minMs` / `late.maxMs` | `1000` / `10000` | Lateness drawn uniformly from `[min,max]` ms. |
| `time.characteristic` | `EVENT_TIME` | `EVENT_TIME` (watermark-driven) or `PROCESSING_TIME` (wall-clock; no watermarks/late data). |
| `watermark.strategy` | `BOUNDED_OUT_OF_ORDERNESS` | `BOUNDED_OUT_OF_ORDERNESS`, `MONOTONOUS`, or `NONE`. Event-time only. |
| `watermark.boundedOutOfOrdernessMs` | `5000` | Out-of-orderness bound for the bounded strategy. |
| `watermark.idlenessMs` | `0` | Mark a subtask idle after this many ms with no data; `0` = off. |
| `window.allowedLatenessMs` | `0` | Keep event-time windows open this long past the watermark for late records. |

### Window & UDF
| Key | Default | Meaning |
|---|---|---|
| `window.type` | `TUMBLING_TIME` | `TUMBLING_TIME`, `TUMBLING_COUNT`, or `SESSION`. |
| `window.sizeMs` | `5000` | Tumbling-time window length. |
| `window.countSize` | `1000` | Tumbling-count window size (records). |
| `window.sessionGapMs` | `5000` | Session inactivity gap. |
| `udf.cpuBurnMicros` | `0` | Busy-spin µs **per record** in the aggregator (simulate CPU-heavy logic). |
| `udf.stateAccumulatorBytes` | `0` | Bytes retained **per accumulator** (inflate window state). |

### Sink, state, checkpointing, restart
| Key | Default | Meaning |
|---|---|---|
| `sink.mode` | `METRICS` | `METRICS` (latency histogram + counters) or `CONSOLE` (log lines). |
| `state.backend` | `HASHMAP` | `HASHMAP` (heap) or `ROCKSDB` (on-disk). |
| `state.rocksdb.incremental` | `true` | Incremental RocksDB checkpoints. |
| `checkpoint.enabled` | `false` | Turn on checkpointing (**required for stateful recovery**). |
| `checkpoint.dir` | `""` | Checkpoint URI, e.g. `s3://flink-checkpoints/harness`. Required when enabled. |
| `checkpoint.intervalMs` | `10000` | Checkpoint interval. |
| `checkpoint.timeoutMs` | `600000` | Checkpoint timeout. |
| `checkpoint.minPauseMs` | `0` | Minimum pause between checkpoints. |
| `restart.strategy` | `fixed-delay` | `none`, `fixed-delay`, or `exponential-delay`. |
| `restart.attempts` | `10` | Max restart attempts (fixed-delay). |
| `restart.delayMs` | `5000` | Delay between restarts. |

### Faults — see the [next section](#fault-injection-explained) for the full story
| Key | Default | Meaning |
|---|---|---|
| `fault.app.enabled` | `false` | Enable the **application-exception** fault. |
| `fault.sys.enabled` | `false` | Enable the **system-halt (TM crash)** fault. |
| `fault.{app,sys}.stage` | `AFTER_SOURCE` | Where to inject: `SOURCE`, `AFTER_SOURCE`, `AGGREGATE`, `BEFORE_SINK`, `SINK`. |
| `fault.{app,sys}.triggerMode` | `EVERY_MS` | `EVERY_N_RECORDS` or `EVERY_MS`. |
| `fault.{app,sys}.triggerN` | `60000` app / `120000` sys | Records or ms between triggers. |
| `fault.{app,sys}.maxOccurrences` | `1` | Cap on how many times it fires per subtask; `≤0` = unlimited. |
| `fault.{app,sys}.probability` | `1.0` | Probability it fires once due (0–1). |
| `fault.sys.haltExitCode` | `1` | JVM exit code for the halt. |

> **Validation:** bad values fail fast at startup with a clear message (e.g. an invalid
> enum lists the allowed values; `checkpoint.enabled=true` requires `checkpoint.dir`; skew
> and late ranges are bounds-checked). See `HarnessConfig.validate()`.

---

## Fault injection explained

The harness can inject two **kinds** of fault, each targetable to any of **five stages**:

- **`fault.app`** — an **application exception** (`HarnessInjectedException`). Flink treats
  it like any operator failure: it triggers a **region failover / restart** governed by
  `restart.strategy`. Use it to test how the cluster recovers from transient code errors.
- **`fault.sys`** — a **system halt**: `Runtime.halt(exitCode)` abruptly kills the
  **TaskManager JVM** (no shutdown hooks). This is the closest thing to "a node died"
  without needing any Kubernetes permissions. Use it to test TM-loss recovery and state
  restore from checkpoint.

Each fault independently chooses:

- **`stage`** — *where*. `SOURCE`, `AGGREGATE`, `SINK` inject **inside** the real operator
  (precise: "the aggregator itself failed"); `AFTER_SOURCE`, `BEFORE_SINK` inject in a
  dedicated pass-through map at the stream boundary.
- **`triggerMode` + `triggerN`** — *how often*. Every N records, or every N milliseconds.
- **`maxOccurrences`** — *how many times* (per subtask).
- **`probability`** — *how reliably* it fires once the trigger condition is met.

All of this logic lives in one place, `FaultInjector` (a pure, unit-tested scheduler). The
boundary operator `FaultInjectionMap` and the three in-operator hooks all delegate to it,
so behaviour is identical wherever you inject.

> **Important limitation:** `fault.app` is *always* an exception and `fault.sys` is *always*
> a halt. So at most **one exception fault and one halt fault** can be active at once, each
> on its own stage. (To inject two exceptions at two stages you'd extend the config — see
> [Extending](#extending-the-harness).)

> **Recovery testing tip:** the synthetic source has **no checkpointed offsets** — on
> restart it regenerates from scratch. So an `EVERY_N_RECORDS` fault will re-fire at the
> same count after every restart (an infinite loop unless `maxOccurrences` caps it). For
> recovery scenarios prefer `triggerMode=EVERY_MS` with a small `maxOccurrences`, and
> always enable checkpointing so state actually survives.

---

## Time & watermarks explained

This is the part newcomers find hardest, so here is exactly what the knobs do.

- **`time.characteristic=EVENT_TIME`** (default): windows are assigned by each record's
  `eventTimeMs`. Flink needs **watermarks** to know when a window is complete. The harness
  assigns timestamps and a watermark generator before `keyBy`.
- **`time.characteristic=PROCESSING_TIME`**: windows are assigned by the machine's
  wall-clock when the record arrives. **No watermarks, no late data** — those knobs become
  inert. Processing-time windows fire on wall-clock timers, which behave *very differently*
  under backpressure (a great A/B axis).

Watermark strategies (event-time only):

- **`BOUNDED_OUT_OF_ORDERNESS`**: allow records up to `boundedOutOfOrdernessMs` out of
  order. The standard choice when data can be slightly late.
- **`MONOTONOUS`**: assume timestamps never go backwards (tightest, lowest latency).
- **`NONE`**: emit no watermarks; event-time windows then fire only at end-of-stream.

`watermark.idlenessMs` marks a quiet subtask idle so it doesn't hold back the overall
watermark. `window.allowedLatenessMs` keeps event-time windows open past the watermark to
still absorb late records.

To keep an A/B fair, set `boundedOutOfOrdernessMs ≥ late.maxMs` so injected late data is
not silently dropped (the `late-data` scenario does this).

---

## Scenario presets

A **scenario** is a YAML file of preset knob values in
`src/main/resources/scenarios/`. `--scenario NAME` loads it, then any explicit `--key value`
on the command line **overrides** individual keys. This gives you repeatable, named runs
for a clean A/B (run the *same* scenario on both clusters).

| Scenario | What it stresses | Key settings |
|---|---|---|
| **`latency-soak`** | Steady moderate load; watch p50/p99/p999 latency over a long run. | `CONSTANT` 50k/s, 1 s windows, RocksDB. |
| **`ceiling-ramp`** | Find the max sustainable throughput. | `RAMP` +5k/s every 15 s; watch where throughput plateaus and backpressure hits 1000 ms/s. |
| **`backpressure`** | Stability under sustained overload. | `MAX` rate + 50 µs CPU-burn so the aggregator is the bottleneck. |
| **`skew-hotkey`** | Load imbalance from hot keys. | 90% of traffic to 5 keys, `KEY_BY`; compare per-subtask busy time. |
| **`late-data`** | Watermarks & late-event handling. | 10% late (1–15 s), session windows, `allowedLateness`. |
| **`processing-time`** | Processing-time vs event-time behaviour. | `PROCESSING_TIME` tumbling windows, no watermarks. |
| **`kill-recovery`** | Fault recovery & state restore. | Checkpointing on; **`fault.sys` at `AGGREGATE`** (TM crash on the stateful operator) + **`fault.app` at `SOURCE`**. Override `checkpoint.dir` for your env. |

Example — run the ramp on the command line, but with a smaller record:

```
--scenario ceiling-ramp --source.recordSizeBytes 128
```

---

## Build & test

You do **not** need Java 11 on your host — everything builds and tests inside a Maven
JDK-11 container that matches the Flink 1.18 runtime. (Building with a newer host JDK can
hit module-access errors, so always use the container.)

```powershell
# Build + 68 unit tests + 12 Flink MiniCluster integration tests + coverage report
docker run --rm -v ${PWD}:/work -v maven-repo:/root/.m2 -w /work `
  maven:3.9-eclipse-temurin-11 mvn -B clean verify
```

Outputs:

- `target/flink-stress-harness.jar` — the shaded ("fat") job jar.
- `target/site/jacoco/index.html` — code-coverage report (~86% instruction coverage).

Test naming convention: `*Test.java` are **unit** tests (Surefire, `test` phase);
`*ITCase.java` are **integration** tests that spin up a real in-process Flink
**MiniCluster** (Failsafe, `verify` phase).

Scorecard (Python) tests:

```powershell
pip install -r scorecard/requirements.txt
pytest scorecard/test_compare.py -q
```

---

## Local full-stack simulation (Docker Compose)

`docker-compose.yml` brings up the complete observability stack so you can watch the job
behave end-to-end on your laptop:

- **jobmanager** + **taskmanager** — Flink running the job (application mode). The default
  command runs `--scenario latency-soak`.
- **prometheus** — scrapes Flink's `/metrics` (port 9249) every 5 s.
- **grafana** — dashboard pre-provisioned, reachable on **:3001** (host :3000 is often
  taken).
- **minio** + **minio-setup** — S3-compatible storage and a one-shot job that creates the
  `flink-checkpoints` bucket, so checkpoint-enabled scenarios work locally.

```powershell
docker compose up --build -d
docker compose logs -f jobmanager      # follow the job
docker compose down -v                 # stop & wipe
```

To switch scenarios, edit the `--scenario ...` in `jobmanager.command` and recreate:

```powershell
docker compose up -d --force-recreate jobmanager taskmanager
```

---

## Reading the Grafana dashboard

Open Grafana (**:3001** locally) → dashboard **"Flink Stress Harness"**. Panels and how to
read them:

| Panel | What it tells you |
|---|---|
| **Source throughput** | Records/sec the source is emitting. If this plateaus while you ramp, you've hit a limit. |
| **End-to-end processed throughput** | Records/sec actually completing through the sink. Should track the source unless backlogged. |
| **End-to-end latency (p50/p99/p999)** | How long records take source→sink (includes window buffering). The headline latency signal. |
| **Backpressure & busy time** | Both are ms/sec out of 1000. **Busy → 1000** = operator saturated. **Backpressured → 1000** = it's being held up by something downstream. |
| **TaskManager CPU load** | JVM CPU % — the resource-efficiency signal. |
| **TaskManager heap used** | Memory pressure. |
| **Checkpoint duration** | How long checkpoints take (state-backend / storage health). |
| **Job restarts** | Increments on every recovery — your fault-recovery signal. |
| **Completed checkpoints** | Should climb steadily when checkpointing is on. |

For an A/B, deploy to both clusters and put the two Grafanas side by side, **or** use the
scorecard for an objective verdict.

---

## Deploying to Kubernetes (Helm)

> **Prerequisite:** the **Flink Kubernetes Operator 1.12** must already be installed
> cluster-wide on each target cluster. This chart ships only the `FlinkDeployment` custom
> resource (which the operator reconciles into JM/TM pods) plus a self-contained
> Prometheus + Grafana.

```powershell
# Build & push the image to a registry both clusters can pull from, then on EACH cluster:
helm install harness helm/flink-stress-harness `
  --set image.repository=<registry>/flink-stress-harness --set image.tag=<tag> `
  --set harness.scenario=latency-soak `
  --set state.backend=rocksdb `
  --set checkpoint.enabled=true --set checkpoint.dir=s3://flink-checkpoints/harness `
  --set s3.endpoint=http://minio.minio.svc:9000 `
  --set s3.accessKey=<key> --set s3.secretKey=<secret>
```

**Use identical values on both clusters** so the only variable is the cluster itself.
`helm/flink-stress-harness/values.yaml` documents every setting. Quick `lint`/render
without a cluster:

```powershell
docker run --rm -v ${PWD}:/apps -w /apps alpine/helm:3.14.4 lint helm/flink-stress-harness
docker run --rm -v ${PWD}:/apps -w /apps alpine/helm:3.14.4 template t helm/flink-stress-harness
```

> **Note on `flink-rbac.yaml`:** in application mode the **JobManager is a Kubernetes API
> client** — it creates TaskManager pods, a ConfigMap (HA/leader election) and a Service at
> runtime, so it needs a ServiceAccount + Role granting that. This is standard Kubernetes
> RBAC and applies to on-prem clusters exactly as to cloud ones. The Flink Operator's own
> chart often *also* provisions a `flink` ServiceAccount; if so, this template may be
> redundant — point the `FlinkDeployment` at the operator's SA and disable this one.

---

## The A/B scorecard

`scorecard/compare.py` queries the Prometheus of **both** clusters over a time window,
aggregates a set of signal metrics, and asserts the candidate is within tolerance of the
baseline. It prints a table and **exits non-zero on any regression** — drop it into a CI
gate.

```powershell
python scorecard/compare.py `
  --baseline  http://<clusterA-prometheus>:9090 `
  --candidate http://<clusterB-prometheus>:9090 `
  --tolerances scorecard/tolerances.yaml --window 10m
```

Metrics, how each is aggregated, the comparison direction (higher- or lower-is-better) and
the tolerance per metric are all declared in `scorecard/tolerances.yaml` — edit that file,
no code change needed. Example output:

```
METRIC                                 BASELINE      CANDIDATE    DELTA%   STATUS
---------------------------------------------------------------------------------
throughput_processed_per_sec          21302.683      20531.783      -3.6     PASS
latency_p99_ms                         2723.000       2901.000      +6.5     PASS
tm_cpu_load_pct                          10.671         13.400     +25.6     FAIL
...
OVERALL: FAIL  (1 regression(s) of 7 metrics)
```

---

## Metrics reference (PromQL)

| Signal | PromQL |
|--------|--------|
| Source throughput | `sum(flink_taskmanager_job_task_numRecordsOutPerSecond{task_name=~".*synthetic.source.*"})` |
| End-to-end throughput | `sum(flink_taskmanager_job_task_operator_processedRecordsPerSec{operator_name=~".*latency.sink.*"})` |
| E2E latency p99 | `max(flink_taskmanager_job_task_operator_e2eLatencyMs{operator_name=~".*latency.sink.*",quantile="0.99"})` |
| Backpressure / busy | `flink_taskmanager_job_task_backPressuredTimeMsPerSecond`, `..._busyTimeMsPerSecond` |
| CPU / heap | `flink_taskmanager_Status_JVM_CPU_Load`, `flink_taskmanager_Status_JVM_Memory_Heap_Used` |
| Checkpoints | `flink_jobmanager_job_lastCheckpointDuration`, `..._numberOfCompletedCheckpoints` |
| Recovery (restarts) | `flink_jobmanager_job_numRestarts` |

> **Gotcha (this wastes hours if you don't know it):** Flink's Prometheus reporter
> **sanitizes label values** — spaces and `-` become `_`, and operators get a
> `Sink:` / `Source:` prefix (e.g. your `latency-sink` operator appears as
> `operator_name="Sink:_latency_sink"`). **Always use regex selectors**
> (`operator_name=~".*latency.sink.*"`), never exact names. The dashboard and scorecard
> already do this.

---

## Project layout

```
pom.xml                        Maven build (shaded fat jar, surefire + failsafe, jacoco)
Dockerfile                     Multi-stage: build jar -> Flink 1.18 runtime + S3 plugin
docker-compose.yml             Full local simulation stack (Flink+Prometheus+Grafana+MinIO)
docker/                        prometheus.yml + grafana provisioning for compose
index.html                     Standalone HTML handover doc (open in a browser)

src/main/java/com/flinkstress/harness/
  HarnessJob.java              Entry point; builds env + pipeline, watermark strategy
  config/                      HarnessConfig (all knobs + validation), enums, ScenarioLoader
  model/                       Event (carries emitTimeMs/eventTimeMs), AggResult
  source/                      RateController, RecordFactory, SyntheticSource
  keyby/                       HarnessKeySelector (KEY_BY | REBALANCE)
  window/                      WindowFactory, WeightedAggregateFunction, KeyStampWindowFunction
  sink/                        LatencyMeasuringSink, SlidingHistogram, ConsoleSink
  fault/                       FaultInjector (shared scheduler), FaultInjectionMap (boundary
                               operator), FaultConfig, HaltStrategy/JvmHaltStrategy,
                               HarnessInjectedException
src/main/resources/scenarios/  7 scenario presets (*.yaml)
src/test/java/...              68 unit tests + 12 MiniCluster integration tests

helm/flink-stress-harness/     Chart: FlinkDeployment CR + Prometheus + Grafana + RBAC
dashboards/                    Grafana dashboard JSON (shared by compose & Helm)
scorecard/                     compare.py + test_compare.py + tolerances.yaml
.claude/CLAUDE.md              Deep guide for AI agents / maintainers
```

---

## Extending the harness

Common changes and where to make them (all are small, localized edits):

- **Add a config knob:** add a `public final` field + a `p.get...("key", default)` line in
  `config/HarnessConfig.java`; add a bounds check in `validate()` if needed; read it where
  relevant. Add a case to `HarnessConfigTest`.
- **Add a window type:** add the enum value to `config/WindowType.java` and a `case` in
  `window/WindowFactory.java`. Add an integration test in `HarnessJobBehaviorITCase`.
- **Add a rate pattern:** add to `config/RateMode.java` and `source/RateController.java`
  (a pure function of elapsed time — easy to unit test in `RateControllerTest`).
- **Add a fault stage:** add to `config/FaultStage.java`; if it's a new in-operator point,
  call `FaultInjector.forStage(cfg, STAGE)` inside that operator and invoke `onRecord()`.
- **Allow two exceptions (or two halts) at once:** generalize `fault.app`/`fault.sys` in
  `HarnessConfig` from two fixed slots into a list of `FaultConfig`.
- **Add a scorecard metric:** add an entry to `scorecard/tolerances.yaml` (no code change).

**House rules** (see `.claude/CLAUDE.md` for the full list): target Flink 1.18 exactly
(`open(Configuration)`, not `open(OpenContext)`); keep behavioural knobs flowing as **job
args** (not cluster config); keep the testability seams pure (`FaultInjector.evaluate`,
`LatencyMeasuringSink.latencyMs`); don't depend on Flink-internal classes.

---

## Troubleshooting & FAQ

**The Grafana panels are empty.** Give it ~30 s after the job reaches `RUNNING` (check the
Flink UI at :8081). If still empty, your PromQL almost certainly uses exact label names —
see the [label-sanitization gotcha](#metrics-reference-promql). Confirm Prometheus is
scraping at http://localhost:9090/targets.

**`docker compose up` fails on the Grafana port.** Host port 3000 is in use; the compose
file already maps Grafana to **3001**. If 3001 is also taken, change the mapping.

**The build fails with module/`OpenContext`/JDK errors.** You're building with a host JDK
newer than 11. Use the Maven JDK-11 container command shown in [Build & test](#build--test).

**A recovery scenario loops forever.** An `EVERY_N_RECORDS` fault re-fires at the same
count after every restart because the source isn't replayable. Use `EVERY_MS` and a small
`maxOccurrences`. See the [recovery tip](#fault-injection-explained).

**Checkpointing won't start.** `checkpoint.enabled=true` requires a non-empty
`checkpoint.dir`, and the bucket must exist (compose's `minio-setup` creates it locally).

**Latency looks huge.** Remember it *includes* window buffering by design — a 5 s window
adds up to ~5 s. It's a relative A/B number, not an SLA.

**Can I run without Kubernetes?** Yes — that's exactly what Docker Compose is for. K8s/Helm
is only for the real A/B against two clusters.

---

## What is actually verified

Being precise so the next maintainer isn't surprised:

- **Automated tests (80):** rate-curve math, record/skew/late statistics (over seeded
  samples), histogram quantiles, fault scheduling, *and* full-job MiniCluster runs proving
  exact record counts, late-data-without-loss, session grouping, skew→subtask
  concentration, rebalance evenness, **in-operator fault-fires (SOURCE/AGGREGATE)**, and
  **restart recovery**. ~86% instruction coverage.
- **Live on Docker Compose:** only the **`latency-soak`** scenario has been run end-to-end
  with real metrics flowing to Prometheus/Grafana and the scorecard validated against live
  data. The other six scenarios are proven at the MiniCluster + config level but have not
  yet been driven through the full compose stack.
- **Not yet exercised live:** a real `Runtime.halt()` TM crash recovering on the dashboard,
  S3/MinIO checkpoint write+restore, and the RAMP/BURST shapes rendered on Grafana. These
  are wired and unit/IT-tested but not visually confirmed end-to-end.

---

## Requirements

- **Docker** (Desktop or engine) — for build, tests, local simulation, and Helm lint.
- **A Flink Kubernetes Operator 1.12** install on each target cluster — for the Helm A/B.
- **Python 3.9+** — for the scorecard.

No local Java or Maven needed (the build runs in a container); no Flink install needed.
