package com.flinkstress.harness.source;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.fault.FaultInjector;
import com.flinkstress.harness.model.Event;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.locks.LockSupport;

/**
 * Parallel synthetic event source. Each subtask paces itself to its share of
 * the job-wide target rate using a token bucket driven by {@link RateController}.
 *
 * <p>Supported firing modes (via config): CONSTANT, RAMP, MAX (unpaced), BURST.
 * The configured rate is the <em>aggregate</em> across all subtasks; each
 * subtask emits {@code rate / parallelism}. A per-subtask deterministic seed
 * keeps the generated stream reproducible for clean A/B comparison.
 */
public class SyntheticSource extends RichParallelSourceFunction<Event> {

    private static final long serialVersionUID = 1L;
    private static final int MAX_BATCH = 4096;
    private static final long PARK_NANOS = 200_000L; // 200us when no tokens available

    private final HarnessConfig cfg;
    private volatile boolean running = true;

    public SyntheticSource(HarnessConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        final int subtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        final Random rnd = new Random(cfg.seed + subtaskIndex);
        final RecordFactory factory = new RecordFactory(cfg, rnd);
        final RateController controller = new RateController(cfg);
        final FaultInjector sourceFault = FaultInjector.forStage(cfg, FaultStage.SOURCE);
        final boolean faultEnabled = sourceFault.hasFaults();
        if (faultEnabled) {
            sourceFault.open(subtaskIndex);
        }

        final long subtaskMaxRecords = perSubtaskMax(cfg.maxRecords, subtasks, subtaskIndex);

        final long startNanos = System.nanoTime();
        long lastNanos = startNanos;
        double tokens = 0.0;
        long localSeq = 0L;

        while (running && (subtaskMaxRecords == 0 || localSeq < subtaskMaxRecords)) {
            long nowNanos = System.nanoTime();
            long elapsedMs = (nowNanos - startNanos) / 1_000_000L;

            if (!controller.isPaced()) {
                long globalSeq = subtaskIndex + localSeq * subtasks;
                Event e = factory.create(globalSeq, System.currentTimeMillis());
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(e);
                }
                localSeq++;
                if (faultEnabled) {
                    sourceFault.onRecord();
                }
                continue;
            }

            double jobRate = controller.currentRatePerSec(elapsedMs);
            double subtaskRate = jobRate / subtasks;
            tokens += ((nowNanos - lastNanos) / 1_000_000_000.0) * subtaskRate;
            lastNanos = nowNanos;
            // cap backlog to ~1s so a backpressure release does not cause an unbounded burst
            double cap = Math.max(1.0, subtaskRate);
            if (tokens > cap) {
                tokens = cap;
            }

            int batch = (int) Math.min(tokens, MAX_BATCH);
            if (subtaskMaxRecords > 0) {
                batch = (int) Math.min(batch, subtaskMaxRecords - localSeq);
            }

            if (batch <= 0) {
                LockSupport.parkNanos(PARK_NANOS);
                continue;
            }

            synchronized (ctx.getCheckpointLock()) {
                for (int i = 0; i < batch && running; i++) {
                    long globalSeq = subtaskIndex + localSeq * subtasks;
                    ctx.collect(factory.create(globalSeq, System.currentTimeMillis()));
                    localSeq++;
                    tokens -= 1.0;
                    if (faultEnabled) {
                        sourceFault.onRecord();
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    /** Splits a job-wide record budget across subtasks (remainder to the lowest indices). */
    static long perSubtaskMax(long jobMax, int subtasks, int subtaskIndex) {
        if (jobMax <= 0) {
            return 0L;
        }
        long base = jobMax / subtasks;
        long remainder = jobMax % subtasks;
        return base + (subtaskIndex < remainder ? 1L : 0L);
    }
}
