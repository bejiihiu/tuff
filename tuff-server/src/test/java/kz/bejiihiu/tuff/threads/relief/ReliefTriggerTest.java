package kz.bejiihiu.tuff.threads.relief;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.threads.observe.TickOrigin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The overrun trigger: hysteresis, cooldown, rate limit, independence and failure isolation.
 *
 * <p>Every case drives the trigger through the same 8 field callback the region handle calls, with a
 * controlled event clock, so nothing depends on a real tick or on wall time.
 */
@Timeout(30)
class ReliefTriggerTest {

    private static final long CALM = 1_000_000L;
    private static final long LONG_BODY = 60_000_000L;

    @Test
    void aWindowOpensOnlyAfterTheConfiguredRunOfOverruns() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));

        fixture.feed.overrun(1L);
        fixture.feed.overrun(1L);
        assertTrue(recorder.opened.isEmpty(), "two overruns are below the enter threshold of three");
        assertEquals(0, fixture.trigger.snapshot().openWindows());

        fixture.feed.overrun(1L);

        assertEquals(1, recorder.opened.size());
        final ReliefWindow window = recorder.opened.getFirst();
        assertEquals(1L, window.regionId());
        assertEquals(1L, window.epoch());
        assertEquals(1, fixture.trigger.snapshot().openWindows());
        assertEquals(1L, fixture.trigger.windowEpoch(1L));
    }

    @Test
    void aCalmBodyResetsTheEnterCounter() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));

        fixture.feed.overrun(1L);
        fixture.feed.overrun(1L);
        fixture.feed.calm(1L);
        fixture.feed.overrun(1L);
        fixture.feed.overrun(1L);
        assertTrue(recorder.opened.isEmpty(), "the calm body restarted the run, so only two overruns are consecutive");

        fixture.feed.overrun(1L);
        assertEquals(1, recorder.opened.size());
    }

    @Test
    void theWindowClosesOnlyAfterTheConfiguredRunOfCalmBodies() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        fixture.openWindow(1L);

        for (int calm = 0; calm < 5; calm++) {
            fixture.feed.calm(1L);
        }
        assertTrue(recorder.closed.isEmpty(), "five calm bodies are below the exit threshold of six");
        assertEquals(1, fixture.trigger.snapshot().openWindows());

        fixture.feed.calm(1L);

        assertEquals(1, recorder.closed.size());
        assertEquals(0, fixture.trigger.snapshot().openWindows());
        assertEquals(1L, fixture.trigger.snapshot().windowsExited());
        assertEquals(0L, fixture.trigger.windowEpoch(1L));
    }

    @Test
    void anOverrunRestartsTheCalmRun() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        fixture.openWindow(1L);

        for (int calm = 0; calm < 3; calm++) {
            fixture.feed.calm(1L);
        }
        fixture.feed.overrun(1L);
        for (int calm = 0; calm < 5; calm++) {
            fixture.feed.calm(1L);
        }
        assertTrue(recorder.closed.isEmpty(), "the overrun restarted the calm run");

        fixture.feed.calm(1L);
        assertEquals(1, recorder.closed.size());
    }

    @Test
    void theCooldownBlocksTheNextWindow() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        fixture.openWindow(1L);
        fixture.closeWindow(1L);

        fixture.feed.overrun(1L);
        fixture.feed.overrun(1L);
        fixture.feed.overrun(1L);

        assertEquals(1, recorder.opened.size(), "the cooldown must keep the second window closed");
        assertTrue(fixture.trigger.snapshot().suppressedByCooldown() >= 1L);

        fixture.feed.advance(Duration.ofSeconds(31L));
        fixture.feed.overrun(1L);

        assertEquals(2, recorder.opened.size());
        assertEquals(2L, recorder.opened.get(1).epoch());
    }

    @Test
    void thePerRegionRateBlocksAWindowEvenAfterTheCooldown() {
        final RegionReliefSettings.Trigger policy = new RegionReliefSettings.Trigger(
            true,
            Duration.ofMillis(40L),
            3,
            6,
            Duration.ZERO,
            2,
            Duration.ofMinutes(5L),
            Duration.ofSeconds(10L)
        );
        final Fixture fixture = new Fixture(policy);
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        fixture.openWindow(1L);
        fixture.closeWindow(1L);
        fixture.openWindow(1L);
        fixture.closeWindow(1L);
        assertEquals(2, recorder.opened.size());

        fixture.feed.overrun(1L);
        fixture.feed.overrun(1L);
        fixture.feed.overrun(1L);

        assertEquals(2, recorder.opened.size(), "two windows are the configured maximum of the rate window");
        assertTrue(fixture.trigger.snapshot().suppressedByRate() >= 1L);

        fixture.feed.advance(Duration.ofMinutes(6L));
        fixture.feed.overrun(1L);

        assertEquals(3, recorder.opened.size());
    }

    @Test
    void regionsKeepIndependentState() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));

        fixture.openWindow(1L);
        fixture.feed.overrun(2L);
        fixture.feed.overrun(2L);
        assertTrue(recorder.opened.stream().noneMatch(window -> window.regionId() == 2L),
            "region one being behind must not open a window for region two");
        assertEquals(1L, fixture.trigger.windowEpoch(1L));
        assertEquals(0L, fixture.trigger.windowEpoch(2L));

        fixture.feed.overrun(2L);

        assertEquals(2, recorder.opened.size());
        assertEquals(1L, fixture.trigger.windowEpoch(2L));
        assertEquals(2, fixture.trigger.snapshot().regions());
        assertEquals(2, fixture.trigger.snapshot().openWindows());
    }

    @Test
    void aDisabledTriggerCallsNothingAndCountsNothing() {
        final Fixture fixture = new Fixture(disabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));

        for (int body = 0; body < 10; body++) {
            fixture.feed.overrun(1L);
        }

        assertTrue(recorder.opened.isEmpty());
        final ReliefTriggerSnapshot snapshot = fixture.trigger.snapshot();
        assertFalse(snapshot.enabled());
        assertEquals(0, snapshot.regions());
        assertEquals(0L, snapshot.windowsEntered());
        assertEquals(0L, snapshot.overrunBodies());
        assertEquals(0L, snapshot.calmBodies());
    }

    @Test
    void aTriggerArmedWithoutReliefStaysOff() {
        final MutableClock clock = new MutableClock(1_000_000_000L);
        final ReliefTrigger trigger = new ReliefTrigger(enabled(), false, clock);
        final Recorder recorder = new Recorder("recorder");
        assertTrue(trigger.register(recorder));

        final Feed feed = new Feed(trigger, clock);
        feed.overrun(1L);
        feed.overrun(1L);
        feed.overrun(1L);

        assertTrue(recorder.opened.isEmpty(), "with relief off, a window would only move work to the end of a tick");
        assertFalse(trigger.enabled());
    }

    @Test
    void noUnitsStillMeansTheMetricsMove() {
        final Fixture fixture = new Fixture(enabled());

        fixture.openWindow(1L);
        fixture.feed.calm(1L);
        fixture.feed.calm(1L);
        fixture.feed.calm(1L);

        final ReliefTriggerSnapshot snapshot = fixture.trigger.snapshot();
        assertTrue(snapshot.enabled());
        assertEquals(0, snapshot.registeredUnits());
        assertEquals(1L, snapshot.windowsEntered());
        assertEquals(3L, snapshot.calmBodies());
        assertEquals(1, snapshot.openWindows());
    }

    @Test
    void aThrowingUnitIsIsolatedAndTheFeedKeepsGoing() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        assertTrue(fixture.trigger.register(new ReliefUnit() {
            @Override
            public String id() {
                return "broken";
            }

            @Override
            public void onWindowOpen(final ReliefWindow window) {
                throw new IllegalStateException("this unit is broken on purpose");
            }

            @Override
            public void onWindowClose(final ReliefWindow window) {
                throw new IllegalStateException("this unit is broken on purpose");
            }
        }));

        fixture.openWindow(1L);

        assertEquals(1, recorder.opened.size(), "the unit after the broken one must still be called");
        assertEquals(1L, fixture.trigger.snapshot().unitFailures());
        assertEquals(2, fixture.trigger.snapshot().registeredUnits());

        fixture.closeWindow(1L);
        assertEquals(1, recorder.closed.size());
        assertEquals(2L, fixture.trigger.snapshot().unitFailures());
    }

    @Test
    void theCallbackRunsOnTheThreadThatProducedTheBody() throws Exception {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        final Thread regionThread = new Thread(() -> fixture.openWindow(4L), "test-region-4");

        regionThread.start();
        regionThread.join(10_000L);

        assertFalse(regionThread.isAlive(), "the feeder thread must finish");
        assertEquals(1, recorder.callers.size());
        assertEquals(regionThread, recorder.callers.getFirst());
        assertEquals(regionThread, recorder.opened.getFirst().owner());
    }

    @Test
    void aDeadlineMissAloneCountsAsAnOverrun() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));

        for (int body = 0; body < 3; body++) {
            fixture.feed.body(1L, CALM, 2L, 0L);
        }

        assertEquals(1, recorder.opened.size(), "a body that starts a whole period behind is an overrun");
    }

    @Test
    void aLateStartAloneCountsAsAnOverrun() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));

        for (int body = 0; body < 3; body++) {
            fixture.feed.body(1L, CALM, 1L, 50_000_000L);
        }

        assertEquals(1, recorder.opened.size(), "a body that starts past the overrun threshold is an overrun");
    }

    @Test
    void theGlobalTickNeverOpensAWindow() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));

        for (int body = 0; body < 5; body++) {
            fixture.feed.overrun(-1L);
        }

        assertTrue(recorder.opened.isEmpty());
        assertEquals(0, fixture.trigger.snapshot().regions());
    }

    @Test
    void aStoppedRegionIsForgottenAndItsWindowIsAbandoned() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        fixture.openWindow(1L);

        fixture.feed.advance(Duration.ofSeconds(11L));
        fixture.feed.calm(2L);

        final ReliefTriggerSnapshot snapshot = fixture.trigger.snapshot();
        assertEquals(1, snapshot.regions(), "the silent region must be dropped from the state");
        assertEquals(0, snapshot.openWindows());
        assertEquals(1L, snapshot.windowsAbandoned());
        assertEquals(0L, fixture.trigger.windowEpoch(1L));
        assertEquals(1, recorder.opened.size(), "an abandoned window must not call close on a foreign thread");
        assertTrue(recorder.closed.isEmpty());
    }

    @Test
    void unregisterStopsTheCallbacks() {
        final Fixture fixture = new Fixture(enabled());
        final Recorder recorder = fixture.register(new Recorder("recorder"));
        assertTrue(fixture.trigger.unregister(recorder));

        fixture.openWindow(1L);

        assertTrue(recorder.opened.isEmpty());
        assertEquals(1L, fixture.trigger.snapshot().windowsEntered(), "the window still opens, nobody is listening");
    }

    @Test
    void theSnapshotListsTheBusiestRegionsFirst() {
        final Fixture fixture = new Fixture(enabled());
        fixture.feed.overrun(9L);
        fixture.feed.overrun(9L);
        fixture.feed.overrun(3L);
        fixture.openWindow(3L);

        final List<ReliefTriggerSnapshot.RegionOffence> offenders = fixture.trigger.snapshot().topOffenders();

        assertEquals(2, offenders.size());
        assertEquals(3L, offenders.getFirst().regionId(), "four overruns beat one");
        assertEquals(9L, offenders.get(1).regionId());
    }

    private static RegionReliefSettings.Trigger enabled() {
        return new RegionReliefSettings.Trigger(
            true,
            Duration.ofMillis(40L),
            3,
            6,
            Duration.ofSeconds(30L),
            4,
            Duration.ofMinutes(5L),
            Duration.ofSeconds(10L)
        );
    }

    private static RegionReliefSettings.Trigger disabled() {
        final RegionReliefSettings.Trigger defaults = RegionReliefSettings.Trigger.defaults();
        return new RegionReliefSettings.Trigger(
            false,
            defaults.overrun(),
            defaults.enterOverruns(),
            defaults.exitCalm(),
            defaults.cooldown(),
            defaults.maxWindows(),
            defaults.rateWindow(),
            defaults.staleAfter()
        );
    }

    /**
     * One trigger plus a recorder and the event clock every case drives.
     */
    private static final class Fixture {

        private final MutableClock clock = new MutableClock(1_000_000_000L);
        private final ReliefTrigger trigger;
        private final Feed feed;

        private Fixture(final RegionReliefSettings.Trigger settings) {
            this.trigger = new ReliefTrigger(settings, true, this.clock);
            this.feed = new Feed(this.trigger, this.clock);
        }

        private Recorder register(final Recorder recorder) {
            assertTrue(this.trigger.register(recorder));
            return recorder;
        }

        private void openWindow(final long regionId) {
            this.feed.overrun(regionId);
            this.feed.overrun(regionId);
            this.feed.overrun(regionId);
        }

        private void closeWindow(final long regionId) {
            for (int calm = 0; calm < 6; calm++) {
                this.feed.calm(regionId);
            }
        }
    }

    /**
     * Drives the callback with controlled timestamps. The clock is the event clock, so a snapshot age
     * and a policy delay never depend on the machine.
     */
    private static final class Feed {

        private final ReliefTrigger trigger;
        private final MutableClock clock;
        private long tickId;

        private Feed(final ReliefTrigger trigger, final MutableClock clock) {
            this.trigger = trigger;
            this.clock = clock;
        }

        private void overrun(final long regionId) {
            this.body(regionId, LONG_BODY, 1L, 0L);
        }

        private void calm(final long regionId) {
            this.body(regionId, CALM, 1L, 0L);
        }

        private void body(final long regionId, final long durationNanos, final long periodsAhead, final long lagNanos) {
            final long start = this.clock.now;
            final long end = start + durationNanos;
            this.tickId++;
            this.trigger.onTick(
                TickOrigin.EDF,
                regionId,
                this.tickId,
                start - lagNanos,
                start,
                end,
                periodsAhead,
                1L
            );
            this.clock.now = end + 1_000_000L;
        }

        private void advance(final Duration gap) {
            this.clock.now += gap.toNanos();
        }
    }

    /**
     * A recorder that keeps every callback in order.
     */
    private static final class Recorder implements ReliefUnit {

        private final String id;
        private final List<ReliefWindow> opened = new ArrayList<>();
        private final List<ReliefWindow> closed = new ArrayList<>();
        private final List<Thread> callers = new ArrayList<>();

        private Recorder(final String id) {
            this.id = id;
        }

        @Override
        public String id() {
            return this.id;
        }

        @Override
        public void onWindowOpen(final ReliefWindow window) {
            this.opened.add(window);
            this.callers.add(Thread.currentThread());
        }

        @Override
        public void onWindowClose(final ReliefWindow window) {
            this.closed.add(window);
        }
    }

    private static final class MutableClock implements LongSupplier {

        private long now;

        private MutableClock(final long start) {
            this.now = start;
        }

        @Override
        public long getAsLong() {
            return this.now;
        }
    }
}
