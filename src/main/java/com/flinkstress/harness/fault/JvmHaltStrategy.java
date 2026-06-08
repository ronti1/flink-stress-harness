package com.flinkstress.harness.fault;

/**
 * Real halt: terminates the TaskManager JVM immediately via
 * {@link Runtime#halt(int)}. Unlike {@code System.exit}, this skips shutdown
 * hooks and finalizers, producing an abrupt crash that exercises Flink's
 * failover and recovery path. Requires no Kubernetes RBAC.
 */
public class JvmHaltStrategy implements HaltStrategy {

    private static final long serialVersionUID = 1L;

    @Override
    public void halt(int exitCode) {
        Runtime.getRuntime().halt(exitCode);
    }
}
