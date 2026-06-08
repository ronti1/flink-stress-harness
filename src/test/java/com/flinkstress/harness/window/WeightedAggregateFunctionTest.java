package com.flinkstress.harness.window;

import com.flinkstress.harness.model.AggResult;
import com.flinkstress.harness.model.Event;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WeightedAggregateFunctionTest {

    private static Event ev(long emitTime, int bytes) {
        return new Event(0, "k", emitTime, emitTime, new byte[bytes]);
    }

    @Test
    void lightAggregateCountsSumsAndTracksMaxEmit() {
        WeightedAggregateFunction f = new WeightedAggregateFunction(0, 0);
        WeightedAggregateFunction.Acc acc = f.createAccumulator();
        f.add(ev(100, 10), acc);
        f.add(ev(300, 20), acc);
        f.add(ev(200, 30), acc);

        AggResult r = f.getResult(acc);
        assertThat(r.count).isEqualTo(3);
        assertThat(r.sumBytes).isEqualTo(60);
        assertThat(r.maxEmitTimeMs).isEqualTo(300);
    }

    @Test
    void emptyAccumulatorYieldsZeroMaxEmit() {
        WeightedAggregateFunction f = new WeightedAggregateFunction(0, 0);
        AggResult r = f.getResult(f.createAccumulator());
        assertThat(r.count).isZero();
        assertThat(r.maxEmitTimeMs).isZero();
    }

    @Test
    void mergeCombinesAccumulators() {
        WeightedAggregateFunction f = new WeightedAggregateFunction(0, 0);
        WeightedAggregateFunction.Acc a = f.createAccumulator();
        WeightedAggregateFunction.Acc b = f.createAccumulator();
        f.add(ev(100, 5), a);
        f.add(ev(500, 7), b);

        WeightedAggregateFunction.Acc merged = f.merge(a, b);
        AggResult r = f.getResult(merged);
        assertThat(r.count).isEqualTo(2);
        assertThat(r.sumBytes).isEqualTo(12);
        assertThat(r.maxEmitTimeMs).isEqualTo(500);
    }

    @Test
    void stateHeavyAllocatesRetainedBuffer() {
        WeightedAggregateFunction f = new WeightedAggregateFunction(0, 4096);
        WeightedAggregateFunction.Acc acc = f.createAccumulator();
        assertThat(acc.retained).isNotNull();
        assertThat(acc.retained.length).isEqualTo(4096);
    }

    @Test
    void lightModeHasNoRetainedBuffer() {
        WeightedAggregateFunction f = new WeightedAggregateFunction(0, 0);
        assertThat(f.createAccumulator().retained).isNull();
    }

    @Test
    void cpuBurnSpendsRoughlyRequestedTime() {
        long micros = 5000; // 5ms
        long start = System.nanoTime();
        WeightedAggregateFunction.busySpinMicros(micros);
        long elapsedMicros = (System.nanoTime() - start) / 1000;
        assertThat(elapsedMicros).isGreaterThanOrEqualTo(micros);
    }
}
