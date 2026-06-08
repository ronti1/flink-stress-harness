package com.flinkstress.harness.fault;

import java.io.Serializable;

/**
 * Abstraction over the JVM-killing action so the system-halt fault can be unit
 * tested without actually terminating the test JVM.
 */
public interface HaltStrategy extends Serializable {
    void halt(int exitCode);
}
