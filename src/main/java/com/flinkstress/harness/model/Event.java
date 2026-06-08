package com.flinkstress.harness.model;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Synthetic event flowing through the harness pipeline.
 *
 * <p>Flink POJO: public no-arg constructor and public fields so the
 * built-in POJO serializer is used (no Kryo fallback).
 */
public class Event implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Monotonic sequence number assigned by the producing subtask. */
    public long seqNo;

    /** Logical key used for keyBy. */
    public String key;

    /** Wall-clock time (ms) when the record was emitted by the source. Basis for end-to-end latency. */
    public long emitTimeMs;

    /** Logical event time (ms). For "late" records this is pushed into the past relative to emit time. */
    public long eventTimeMs;

    /** Opaque payload used purely to control record size on the wire. */
    public byte[] payload;

    public Event() {
        this.payload = new byte[0];
    }

    public Event(long seqNo, String key, long emitTimeMs, long eventTimeMs, byte[] payload) {
        this.seqNo = seqNo;
        this.key = key;
        this.emitTimeMs = emitTimeMs;
        this.eventTimeMs = eventTimeMs;
        this.payload = payload;
    }

    public int sizeBytes() {
        return payload == null ? 0 : payload.length;
    }

    @Override
    public String toString() {
        return "Event{seqNo=" + seqNo
                + ", key=" + key
                + ", emitTimeMs=" + emitTimeMs
                + ", eventTimeMs=" + eventTimeMs
                + ", payloadBytes=" + sizeBytes()
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event event = (Event) o;
        return seqNo == event.seqNo
                && emitTimeMs == event.emitTimeMs
                && eventTimeMs == event.eventTimeMs
                && java.util.Objects.equals(key, event.key)
                && Arrays.equals(payload, event.payload);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(seqNo, key, emitTimeMs, eventTimeMs);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
