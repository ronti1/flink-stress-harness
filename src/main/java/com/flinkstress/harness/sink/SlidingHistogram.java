package com.flinkstress.harness.sink;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.Arrays;

/**
 * Minimal {@link Histogram} backed by a fixed-size sliding reservoir, so the
 * harness has no dependency on Flink-internal histogram classes. Quantiles use
 * the nearest-rank method over a sorted snapshot of the reservoir.
 */
public class SlidingHistogram implements Histogram {

    private final long[] reservoir;
    private long count;
    private int next;
    private boolean filled;

    public SlidingHistogram(int reservoirSize) {
        if (reservoirSize <= 0) {
            throw new IllegalArgumentException("reservoirSize must be > 0");
        }
        this.reservoir = new long[reservoirSize];
    }

    @Override
    public synchronized void update(long value) {
        reservoir[next] = value;
        next = (next + 1) % reservoir.length;
        if (next == 0) {
            filled = true;
        }
        count++;
    }

    @Override
    public synchronized long getCount() {
        return count;
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
        int size = filled ? reservoir.length : next;
        long[] snapshot = Arrays.copyOf(reservoir, size);
        Arrays.sort(snapshot);
        return new SortedStatistics(snapshot);
    }

    private static final class SortedStatistics extends HistogramStatistics {

        private final long[] sorted;

        private SortedStatistics(long[] sorted) {
            this.sorted = sorted;
        }

        @Override
        public double getQuantile(double quantile) {
            if (sorted.length == 0) {
                return 0.0;
            }
            double q = Math.max(0.0, Math.min(1.0, quantile));
            int rank = (int) Math.ceil(q * sorted.length);
            int idx = Math.max(0, Math.min(sorted.length - 1, rank - 1));
            return sorted[idx];
        }

        @Override
        public long[] getValues() {
            return Arrays.copyOf(sorted, sorted.length);
        }

        @Override
        public int size() {
            return sorted.length;
        }

        @Override
        public double getMean() {
            if (sorted.length == 0) {
                return 0.0;
            }
            long sum = 0;
            for (long v : sorted) {
                sum += v;
            }
            return (double) sum / sorted.length;
        }

        @Override
        public double getStdDev() {
            if (sorted.length < 2) {
                return 0.0;
            }
            double mean = getMean();
            double sq = 0.0;
            for (long v : sorted) {
                double d = v - mean;
                sq += d * d;
            }
            return Math.sqrt(sq / (sorted.length - 1));
        }

        @Override
        public long getMax() {
            return sorted.length == 0 ? 0L : sorted[sorted.length - 1];
        }

        @Override
        public long getMin() {
            return sorted.length == 0 ? 0L : sorted[0];
        }
    }
}
