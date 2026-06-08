package com.flinkstress.harness.config;

/** State backend selection (configurable knob to test both). */
public enum StateBackend {
    /** Embedded RocksDB, spills to local disk. */
    ROCKSDB,
    /** In-memory heap (HashMapStateBackend). */
    HASHMAP
}
