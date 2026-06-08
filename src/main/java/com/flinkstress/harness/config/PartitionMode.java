package com.flinkstress.harness.config;

/** How records are distributed across downstream subtasks. */
public enum PartitionMode {
    /** Partition by key (keyBy) &mdash; exercises keyed state and skew. */
    KEY_BY,
    /** Round-robin rebalance &mdash; even distribution, no keyed state. */
    REBALANCE
}
