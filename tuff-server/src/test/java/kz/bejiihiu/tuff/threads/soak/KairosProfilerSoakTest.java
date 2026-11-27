package kz.bejiihiu.tuff.threads.soak;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.threads.kairos.KairosGate;
import kz.bejiihiu.tuff.threads.kairos.KairosTuning;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scheduler with a profiler attached to every tick body.
 *
 * <p>A profiler changes the tick path: it reads a clock or a thread CPU counter on every entry and
 * exit. The test does exactly that from inside the runner thread, which is the closest reproduction of
 * a profiler pass without a build plugin, and then checks the invariants that a profiler must not
 * break: one execution per generation, a positive CPU measurement per execution, and no permit left
 * behind when the last generation finishes.
 *
 * <p>Busy work is used instead of a sleep so the measured CPU time is really CPU time of the tick
 * thread; a sleeping body would legitimately measure zero.
 */
@Tag("soak")
@Timeout(150)
class KairosProfilerSoakTest {

    private KairosRig rig;

    @AfterEach
    void stop() {
        if (this.rig != null) {
            this.rig.close();
            this.rig = null;
        }
        KairosGate.disarm();
    }

    @Test
    void aProfilerOnPassKeepsExactlyOnceAndMeasuresCpu() throws InterruptedException {
        this.rig = new KairosRig(2, KairosTuning.fixed(), SoakFixtures.FaultyThreadFactory.normal(), null);
        this.rig.start();
        final AtomicBoolean profilerOn = new AtomicBoolean(true);
        final int ticks = 8;
        final int reschedules = 3;
        final CountDownLatch done = new CountDownLatch(ticks * (reschedules + 1));
        for (int index = 0; index < ticks; ++index) {
            this.rig.tick(
                "profiled-" + index,
                System.nanoTime(),
                reschedules,
                TimeUnit.MILLISECONDS.toNanos(25L),
                TimeUnit.MILLISECONDS.toNanos(1L),
                done,
                profilerOn::get
            );
        }

        assertTrue(done.await(30L, TimeUnit.SECONDS), "every profiled generation must run");
        long totalCpu = 0L;
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(reschedules + 1, tick.runs(), "a profiled tick still runs exactly once per generation");
            assertTrue(tick.cpuNanos() > 0L, "the profiler must observe positive CPU time on " + tick);
            totalCpu += tick.cpuNanos();
        }
        assertTrue(totalCpu > 0L, "the profiler observed no CPU time at all");
        this.rig.awaitQuiescent(15_000L);
        assertEquals(0, this.rig.permitsInUse(), "a profiled run must not leak a permit");

        profilerOn.set(false);
        final CountDownLatch off = new CountDownLatch(1);
        this.rig.tick("unprofiled", System.nanoTime(), 0, 0L, 0L, off, profilerOn::get);
        assertTrue(off.await(10L, TimeUnit.SECONDS), "the pass must finish with the profiler off too");
        assertEquals(0L, this.rig.ticks().getLast().cpuNanos(), "with the profiler off nothing is measured");
    }
}
