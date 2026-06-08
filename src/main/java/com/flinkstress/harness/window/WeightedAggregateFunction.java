package com.flinkstress.harness.window;

import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.model.AggResult;
import com.flinkstress.harness.model.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Windowed aggregation whose compute and state weight are independently
 * configurable:
 *
 * <ul>
 *   <li><b>light</b> (always): count, sum of payload bytes, max emit time.</li>
 *   <li><b>cpu-burn</b> ({@code udf.cpuBurnMicros} &gt; 0): busy-spin per record
 *       to simulate CPU-heavy logic.</li>
 *   <li><b>state-heavy</b> ({@code udf.stateAccumulatorBytes} &gt; 0): retain a
 *       byte buffer per accumulator to inflate window state.</li>
 * </ul>
 *
 * <p>{@code maxEmitTimeMs} is carried through so the sink can derive end-to-end
 * latency.
 */
public class WeightedAggregateFunction
        implements AggregateFunction<Event, WeightedAggregateFunction.Acc, AggResult> {

    private static final long serialVersionUID = 1L;

    private final long cpuBurnMicros;
    private final int stateAccumulatorBytes;

    public WeightedAggregateFunction(HarnessConfig cfg) {
        this(cfg.cpuBurnMicros, cfg.stateAccumulatorBytes);
    }

    public WeightedAggregateFunction(long cpuBurnMicros, int stateAccumulatorBytes) {
        this.cpuBurnMicros = cpuBurnMicros;
        this.stateAccumulatorBytes = stateAccumulatorBytes;
    }

    /** Mutable accumulator. */
    public static final class Acc {
        public long count;
        public long sumBytes;
        public long maxEmitTimeMs;
        public byte[] retained;
    }

    @Override
    public Acc createAccumulator() {
        Acc acc = new Acc();
        acc.maxEmitTimeMs = Long.MIN_VALUE;
        if (stateAccumulatorBytes > 0) {
            acc.retained = new byte[stateAccumulatorBytes];
        }
        return acc;
    }

    @Override
    public Acc add(Event event, Acc acc) {
        acc.count++;
        acc.sumBytes += event.sizeBytes();
        if (event.emitTimeMs > acc.maxEmitTimeMs) {
            acc.maxEmitTimeMs = event.emitTimeMs;
        }
        if (cpuBurnMicros > 0) {
            busySpinMicros(cpuBurnMicros);
        }
        return acc;
    }

    @Override
    public AggResult getResult(Acc acc) {
        long maxEmit = acc.maxEmitTimeMs == Long.MIN_VALUE ? 0L : acc.maxEmitTimeMs;
        // key + aggTime are filled by the downstream window function
        return new AggResult(null, acc.count, acc.sumBytes, maxEmit, System.currentTimeMillis());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.count += b.count;
        a.sumBytes += b.sumBytes;
        a.maxEmitTimeMs = Math.max(a.maxEmitTimeMs, b.maxEmitTimeMs);
        if (a.retained == null) {
            a.retained = b.retained;
        }
        return a;
    }

    /** Busy-spin for approximately the given number of microseconds. */
    static void busySpinMicros(long micros) {
        long deadline = System.nanoTime() + micros * 1_000L;
        long spins = 0;
        while (System.nanoTime() < deadline) {
            spins++;
            if ((spins & 0xFFFF) == 0) {
                Thread.onSpinWait();
            }
        }
    }
}
