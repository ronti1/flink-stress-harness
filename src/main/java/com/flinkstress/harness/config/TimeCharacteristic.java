package com.flinkstress.harness.config;

/** Notion of time used by time-based windows. */
public enum TimeCharacteristic {
    /** Windows assigned by the record's event time (watermark-driven). */
    EVENT_TIME,
    /** Windows assigned by wall-clock processing time (no watermarks). */
    PROCESSING_TIME
}
