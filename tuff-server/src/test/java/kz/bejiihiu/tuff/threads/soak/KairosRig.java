package kz.bejiihiu.tuff.threads.soak;

import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.threads.kairos.KairosGate;
import kz.bejiihiu.tuff.threads.kairos.KairosScheduler;
import kz.bejiihiu.tuff.threads.kairos.KairosServices;
import kz.bejiihiu.tuff.threads.kairos.KairosSnapshot;
import kz.bejiihiu.tuff.threads.kairos.KairosSupervisorSource;
import kz.bejiihiu.tuff.threads.kairos.KairosTuning;
import org.jspecify.annotations.Nullable;

/**
 * Drives one real {@link KairosScheduler} through its public seams.
 *
 * <p>The rig exists so every soak test starts and stops the scheduler the same way: arm the opt-in
 * gate, give the scheduler the test factory and (optionally) a supervisor source, start it, and on the
 * way out halt, join every thread that ever existed and disarm the gate. A test that forgets the last
 * step would poison the next one through the static gate.
 *
 * <p>{@link #halt()} records the instant after {@code halt()} returned. Ticks created by the rig read
 * that flag, which is how "no tick started after halt" is asserted without timing guesses.
 */
final class KairosRig implements AutoCloseable {

    private final KairosScheduler scheduler;
    private final SoakFixtures.FaultyThreadFactory threads;
    private final AtomicBoolean halted = new AtomicBoolean();
    private final List<SoakFixtures.SoakTick> ticks = Collections.synchronizedList(new ArrayList<>());

    /**
     * Arms the gate and builds a scheduler without starting it.
     *
     * @param floor    initial fund size
     * @param tuning   fund limits
     * @param factory  thread factory, normally a faulty one
     * @param source   supervisor source, or {@code null} for the plain daemon
     */
    KairosRig(
        final int floor,
        final KairosTuning tuning,
        final SoakFixtures.FaultyThreadFactory factory,
        final @Nullable KairosSupervisorSource source
    ) {
        this.threads = factory;
        KairosGate.arm(true, tuning, new KairosServices(null, source == null ? KairosSupervisorSource.plain() : source));
        this.scheduler = new KairosScheduler(floor, factory, this::deadline);
    }

    private long deadline(final SchedulableTick tick) {
        return ((SoakFixtures.SoakTick) tick).deadline();
    }

    /**
     * Starts the fund.
     */
    void start() {
        this.scheduler.start();
    }

    /**
     * Returns the scheduler under test.
     *
     * @return the scheduler
     */
    KairosScheduler scheduler() {
        return this.scheduler;
    }

    /**
     * Returns the factory the scheduler allocates from.
     *
     * @return the factory
     */
    SoakFixtures.FaultyThreadFactory threads() {
        return this.threads;
    }

    /**
     * Returns whether {@link #halt()} already returned.
     *
     * @return the halt flag
     */
    boolean halted() {
        return this.halted.get();
    }

    /**
     * Creates and schedules a tick owned by this rig.
     *
     * <p>The tick is fully configured before it is scheduled, so a running scheduler can never observe
     * a half built tick.
     *
     * @param label        failure message label
     * @param deadline     first deadline, in {@link System#nanoTime()} units
     * @param reschedules  how many extra executions the tick schedules for itself
     * @param runNanos     nanoseconds of busy work per execution
     * @param periodNanos  delay between two executions of a rescheduling tick
     * @param done         latch counted down per execution
     * @param profiler     optional profiler flag; may be {@code null}
     * @return the tick, registered for later accounting
     */
    SoakFixtures.SoakTick tick(
        final String label,
        final long deadline,
        final int reschedules,
        final long runNanos,
        final long periodNanos,
        final CountDownLatch done,
        final @Nullable BooleanSupplier profiler
    ) {
        final SoakFixtures.SoakTick tick = new SoakFixtures.SoakTick(
            label, deadline, reschedules, runNanos, periodNanos, done, this.halted::get, profiler
        );
        this.ticks.add(tick);
        this.scheduler.schedule(tick);
        return tick;
    }

    /**
     * Returns every tick the rig created.
     *
     * @return the ticks, in creation order
     */
    List<SoakFixtures.SoakTick> ticks() {
        synchronized (this.ticks) {
            return List.copyOf(this.ticks);
        }
    }

    /**
     * Returns one consistent snapshot of the fund.
     *
     * @return the snapshot
     */
    KairosSnapshot snapshot() {
        return this.scheduler.snapshot();
    }

    /**
     * Halts the scheduler and records the instant.
     */
    void halt() {
        this.scheduler.halt();
        this.halted.set(true);
    }

    /**
     * Halts the scheduler, waits for every thread that ever existed and checks the join.
     *
     * @param timeoutMillis maximum join wait
     * @throws AssertionError if a thread survives the join
     */
    void haltAndJoin(final long timeoutMillis) {
        this.halt();
        if (!this.scheduler.join(timeoutMillis)) {
            throw new AssertionError(
                "join did not return within " + timeoutMillis + " ms; live threads: "
                    + this.scheduler.liveThreads()
            );
        }
        if (this.scheduler.getAliveThreads().length != 0) {
            throw new AssertionError("getAliveThreads is not empty after a successful join");
        }
        if (this.threads.anyAlive()) {
            throw new AssertionError("a factory-created thread survived the join");
        }
    }

    /**
     * Waits, bounded, until every scheduled tick is finished and the permit counters are back to zero.
     *
     * @param timeoutMillis maximum wait
     * @throws AssertionError if the fund never becomes quiescent
     */
    void awaitQuiescent(final long timeoutMillis) {
        SoakFixtures.await("queued + awaiting + executing == scheduled == 0", timeoutMillis, () -> {
            final KairosSnapshot snapshot = this.snapshot();
            return snapshot.scheduledTicks() == 0
                && snapshot.queuedTicks() + snapshot.awaitingTicks() + snapshot.executingTicks() == 0;
        });
    }

    /**
     * Returns the number of permit holders and waiters in one consistent snapshot.
     *
     * @return queued plus awaiting plus executing
     */
    int permitsInUse() {
        final KairosSnapshot snapshot = this.snapshot();
        return snapshot.queuedTicks() + snapshot.awaitingTicks() + snapshot.executingTicks();
    }

    /**
     * Asserts that the permit counters account for every tick the scheduler still holds.
     *
     * <p>Comparing the sum with itself is what used to hide a cancel that left its state entry behind;
     * the scheduled count is the other side of the equation and is read from the same snapshot.
     *
     * @throws AssertionError if the two sides disagree
     */
    void assertPermitAccounting() {
        final KairosSnapshot snapshot = this.snapshot();
        if (snapshot.scheduledTicks()
            != snapshot.queuedTicks() + snapshot.awaitingTicks() + snapshot.executingTicks()) {
            throw new AssertionError("permit accounting drifted from the scheduled set: " + snapshot);
        }
    }

    /**
     * Halts and joins, then disarms the gate.
     *
     * <p>Never throws: it is the cleanup path of a test that may already be failing.
     */
    @Override
    public void close() {
        try {
            this.scheduler.halt();
            this.scheduler.join(TimeUnit.SECONDS.toMillis(20L));
        } catch (RuntimeException _) {
            // cleanup only: a broken fund is reported by the test that drove it
        } finally {
            KairosGate.disarm();
        }
    }
}
