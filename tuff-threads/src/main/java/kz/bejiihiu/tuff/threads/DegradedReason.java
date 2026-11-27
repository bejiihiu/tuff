package kz.bejiihiu.tuff.threads;

/**
 * Why a pool or the manager cannot be managed as fully as its configuration asks.
 *
 * <p>Degradation is a first class state, not a log line: it shows up in every snapshot, it is included
 * in JFR pool events and the command surface prints it next to the pool. The point is that "observe
 * only" and "we wanted to manage this but could not" look different to an operator, instead of both
 * looking like a healthy zero.
 */
public enum DegradedReason {

    /**
     * The pool is registered at {@link CoverageLevel#L1} and the manager only observes it.
     *
     * <p>Expected for pools whose semantics are not ready for a factory or a budget, and for every pool
     * before its integration phase lands.
     */
    OBSERVE_ONLY,

    /**
     * The feature that would manage this pool is disabled by configuration.
     *
     * <p>For example region execution with {@code managed: false}, which is the safe default.
     */
    FEATURE_DISABLED,

    /**
     * The declared reserves and minimums do not fit into the usable core budget.
     *
     * <p>The manager keeps running with what exists and reports this instead of silently
     * overcommitting. A pool with this reason may be below its declared minimum.
     */
    BUDGET_EXHAUSTED,

    /**
     * The processor topology could not be determined reliably.
     *
     * <p>Typically a container where the OS reports more CPUs than the process may use, or a platform
     * without the native topology probe. Sizing falls back to {@link Runtime#availableProcessors()}.
     */
    TOPOLOGY_UNKNOWN,

    /**
     * The pool owner cannot expose the control the manager would need.
     *
     * <p>The canonical case is Folia's EDF region scheduler: its thread count cannot be changed without
     * a restart, so capacity control is unavailable while it runs.
     */
    SCHEDULER_LIMITATION,

    /**
     * Creating a thread failed, usually because the OS refused it or the JVM is out of memory.
     *
     * <p>The pool keeps the threads it has and the failure is reported once per attempt, not per
     * request.
     */
    THREAD_CREATION_FAILED,

    /**
     * The pool stopped answering probes.
     *
     * <p>Its last known values stay in snapshots but are marked degraded; the manager does not block
     * waiting for a probe that may be stuck.
     */
    POOL_UNRESPONSIVE,

    /**
     * NUMA or affinity information is unavailable on this platform.
     *
     * <p>Not an error on Windows or macOS: topology is reported as a single node and pinning is not
     * attempted.
     */
    NUMA_UNAVAILABLE,

    /**
     * A legacy configuration key and the unified configuration disagree.
     *
     * <p>The unified value wins, as documented for the migration, and the conflict is reported so the
     * operator can remove the stale key.
     */
    CONFIG_CONFLICT,

    /**
     * Metrics collection is disabled, so the pool is registered but cannot be observed.
     */
    METRICS_UNAVAILABLE,

    /**
     * A critical pool (region execution or network) is short of its guaranteed share.
     *
     * <p>Two situations produce it: the pool's lease queue is growing while every permit it owns is
     * taken, or the arithmetic could only honour its declared minimum by overcommitting the ledger. The
     * guarantee itself is not renegotiated - the controller keeps the minimum and, when load shedding is
     * on, takes the missing cores back from the non critical pools. The numbers live in
     * {@link SheddingReport#summary()} and in the pool's saturation, the enum only names the state.
     */
    CRITICAL_POOL_PRESSURE,

    /**
     * The pool's capacity is temporarily reduced to keep a critical lane alive.
     *
     * <p>Only non critical pools get this reason, only while load shedding is active, and never below
     * the pool's declared minimum. It is a policy action, not a failure: the pool keeps its threads and
     * its held permits, it just stops taking new work until the critical queue drains.
     */
    LOAD_SHEDDING,

    /**
     * The manager has not been started yet or has already stopped.
     *
     * <p>Reads still work and return the last known snapshot, but no new leases or registrations are
     * accepted.
     */
    NOT_RUNNING
}
