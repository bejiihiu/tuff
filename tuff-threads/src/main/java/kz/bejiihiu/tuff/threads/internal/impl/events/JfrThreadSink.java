package kz.bejiihiu.tuff.threads.internal.impl.events;

import java.util.StringJoiner;
import jdk.jfr.EventType;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import org.jspecify.annotations.Nullable;

/**
 * {@link MetricsEventSink} backed by the JDK flight recorder.
 *
 * <p>Cost when nothing records the thread layer is one {@link EventType#isEnabled()} check per hook: the
 * event object, the enum names and the degraded list are only built after that check passed. There is no
 * extra rate limit on top of JFR's own threshold, because a hard coded limit would bias a recording;
 * the sampler already only calls this once per sampling window.
 *
 * <p>On a runtime without {@code jdk.jfr} the constructor succeeds anyway: the event types are resolved
 * in an isolated holder whose failure is caught, and every hook becomes a no-op. A server build must
 * never fail to start because profiling is unavailable.
 */
public final class JfrThreadSink implements MetricsEventSink {

    private final @Nullable Types types;

    /**
     * Creates a sink. Never throws, even when the flight recorder module is absent.
     */
    public JfrThreadSink() {
        this.types = Types.load();
    }

    /**
     * Checks whether at least one thread event can be recorded right now.
     *
     * @return {@code true} when the runtime has the flight recorder and a recording enabled an event
     */
    public boolean enabled() {
        final Types current = this.types;
        return current != null && current.anyEnabled();
    }

    @Override
    public void threadCreated(final ThreadSnapshot thread) {
        if (this.types == null || !Types.CREATED.isEnabled()) {
            return;
        }
        try {
            final ThreadCreatedEvent event = new ThreadCreatedEvent();
            event.threadId = thread.threadId();
            event.threadName = thread.name();
            event.poolId = thread.pool().value();
            event.daemon = thread.daemon();
            event.priority = thread.priority();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break thread creation
        }
    }

    @Override
    public void threadTerminated(final ThreadSnapshot thread) {
        if (this.types == null || !Types.TERMINATED.isEnabled()) {
            return;
        }
        try {
            final ThreadTerminatedEvent event = new ThreadTerminatedEvent();
            event.threadId = thread.threadId();
            event.threadName = thread.name();
            event.poolId = thread.pool().value();
            event.state = thread.state().name();
            event.cpuTimeNanos = thread.cpuTimeNanos();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break thread termination
        }
    }

    @Override
    public void poolState(final PoolSnapshot pool) {
        if (this.types == null || !Types.POOL_STATE.isEnabled()) {
            return;
        }
        try {
            final PoolStateEvent event = new PoolStateEvent();
            event.poolId = pool.id().value();
            event.poolKind = pool.kind().name();
            event.coverage = pool.coverage().name();
            event.aliveThreads = pool.aliveThreads();
            event.coreThreads = pool.coreThreads();
            event.activeThreads = pool.activeThreads();
            event.idleThreads = pool.idleThreads();
            event.queuedTasks = pool.queuedTasks();
            event.queueCapacity = pool.queueCapacity();
            event.cpuLoad = pool.cpuLoad();
            event.saturation = pool.saturation().level().name();
            event.degradedReasons = joinReasons(pool);
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a sampling pass
        }
    }

    @Override
    public void saturation(final SaturationReport report) {
        if (this.types == null || !Types.SATURATION.isEnabled()) {
            return;
        }
        try {
            final SaturationEvent event = new SaturationEvent();
            event.poolId = report.pool().value();
            event.level = report.level().name();
            event.utilisation = report.utilisation();
            event.queuePressure = report.queuePressure();
            event.queuedTasks = report.queuedTasks();
            event.longestWaitNanos = report.longestWaitNanos();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a sampling pass
        }
    }

    @Override
    public void budgetChanged(final PoolId pool, final ResourceBudget previous, final ResourceBudget current) {
        if (this.types == null || !Types.BUDGET_CHANGED.isEnabled()) {
            return;
        }
        try {
            final BudgetChangedEvent event = new BudgetChangedEvent();
            event.poolId = pool.value();
            event.previousCores = previous.cores();
            event.cores = current.cores();
            event.previousReservedCores = previous.reservedCores();
            event.reservedCores = current.reservedCores();
            event.previousShare = previous.share();
            event.share = current.share();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a reload
        }
    }

    /**
     * Emits a lease timeout event.
     *
     * <p>Not part of the frozen sink contract, so the controller has to call this method on the concrete
     * type it was given. Public on purpose: the same reason the compute recorder exposes its rejection
     * hook.
     *
     * @param request the request that was not granted
     */
    @Override
    public void leaseTimedOut(final LeaseRequest request) {
        this.leaseTimedOut(request.pool(), request.deadlineNanos(), request.maxWait().toNanos());
    }

    @Override
    public void unregisteredThread(final long threadId, final String name) {
        if (this.types == null || !Types.UNKNOWN_THREADS.isEnabled()) {
            return;
        }
        try {
            final UnknownThreadsEvent event = new UnknownThreadsEvent();
            event.threadId = threadId;
            event.threadName = name;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a scan
        }
    }

    private void leaseTimedOut(final PoolId pool, final long deadlineNanos, final long maxWaitNanos) {
        if (this.types == null || !Types.LEASE_TIMEOUT.isEnabled()) {
            return;
        }
        try {
            final LeaseTimeoutEvent event = new LeaseTimeoutEvent();
            event.poolId = pool.value();
            event.deadlineNanos = deadlineNanos;
            event.maxWaitNanos = maxWaitNanos;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a lease
        }
    }

    private static String joinReasons(final PoolSnapshot pool) {
        final StringJoiner joiner = new StringJoiner(",");
        for (final DegradedReason reason : pool.degradedReasons()) {
            joiner.add(reason.name());
        }
        return joiner.toString();
    }

    /**
     * Lazily resolved flight recorder types.
     *
     * <p>Kept in a nested class so a JVM without {@code jdk.jfr} fails while loading this holder, which
     * the constructor catches, instead of failing while loading {@link JfrThreadSink} itself.
     */
    private static final class Types {

        static final EventType CREATED = EventType.getEventType(ThreadCreatedEvent.class);
        static final EventType TERMINATED = EventType.getEventType(ThreadTerminatedEvent.class);
        static final EventType POOL_STATE = EventType.getEventType(PoolStateEvent.class);
        static final EventType SATURATION = EventType.getEventType(SaturationEvent.class);
        static final EventType BUDGET_CHANGED = EventType.getEventType(BudgetChangedEvent.class);
        static final EventType UNKNOWN_THREADS = EventType.getEventType(UnknownThreadsEvent.class);
        static final EventType LEASE_TIMEOUT = EventType.getEventType(LeaseTimeoutEvent.class);

        private Types() {
        }

        static @Nullable Types load() {
            try {
                return new Types();
            } catch (Throwable _) {
                // the runtime has no flight recorder; every hook becomes a no-op
                return null;
            }
        }

        boolean anyEnabled() {
            return POOL_STATE.isEnabled()
                || CREATED.isEnabled()
                || TERMINATED.isEnabled()
                || SATURATION.isEnabled()
                || BUDGET_CHANGED.isEnabled()
                || UNKNOWN_THREADS.isEnabled()
                || LEASE_TIMEOUT.isEnabled();
        }
    }
}
