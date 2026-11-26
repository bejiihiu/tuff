package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engineThreadNames;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wiring of the two ports: the legacy pair must be indistinguishable from the old code, and a custom
 * pair must really be used by every engine thread and by sizing.
 */
@Timeout(120)
class ComputeSpiPortsTest {

    @Test
    void defaultPortsKeepTheLegacyThreadNames() throws Exception {
        final EngineTestSupport.SettingsBuilder builder = settings().fixedWorkers(2).noVirtualLane();
        final ComputeEngineImpl engine = new ComputeEngineImpl(builder.build(), new EngineTestSupport.FakeClock());
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            assertEquals(2, engine.desiredWorkers());
            final String workerName = compute.submit(() -> Thread.currentThread().getName()).get(10L, TimeUnit.SECONDS);
            assertTrue(workerName.startsWith(builder.prefix() + '-'), workerName);

            // the service threads keep their exact pre-port names, no "-1" suffix appeared
            assertEquals(List.of(builder.prefix() + "-janitor"), engineThreadNames(builder.prefix() + "-janitor"));
        } finally {
            compute.shutdown();
        }
        assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
    }

    @Test
    void budgetPortDrivesSizingAndApplication() throws Exception {
        // one more than the host so a leaked JVM count would size the pool differently
        final int portBudget = Runtime.getRuntime().availableProcessors() + 2;
        final AtomicInteger asked = new AtomicInteger();
        final ComputeBudgetPort budget = new ComputeBudgetPort() {
            @Override
            public int processors() {
                asked.incrementAndGet();
                return portBudget;
            }

            @Override
            public String source() {
                return "test";
            }
        };
        final EngineTestSupport.SettingsBuilder builder = settings()
            .autoWorkers(1, -1, Duration.ofSeconds(30))
            .noVirtualLane();
        final ComputeSettings configured = builder.build();
        final ComputeSettings.Workers workers = configured.workers();
        final int expectedCeiling = portBudget;
        final ComputeEngineImpl engine = new ComputeEngineImpl(
            configured,
            new EngineTestSupport.FakeClock(),
            budget,
            ComputeThreadFactoryPort.legacy()
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        assertEquals(1, asked.get(), "the port is the single budget source and is asked once");
        compute.start();
        final CountDownLatch release = new CountDownLatch(1);
        try {
            for (int index = 0; index < expectedCeiling + 1; index++) {
                compute.submit(() -> {
                    release.await();
                    return 1;
                });
            }
            // growth stops at the port's budget; a leaked JVM count would stop at its own ceiling
            assertEventually("the port budget caps pool growth", Duration.ofSeconds(10), () ->
                engine.desiredWorkers() == expectedCeiling);
            assertTrue(expectedCeiling > workers.resolveMaxThreads(Runtime.getRuntime().availableProcessors()));
            assertEquals(1, asked.get(), "growing the pool never re-reads the port");

            // runtime updates still go through the documented path and keep the same formula
            engine.applyProcessorBudget(1);
            assertEventually("a smaller budget still applies", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() == 1 && engine.totalLimit() == 1);
            assertEquals(1, asked.get(), "the runtime path takes a number, it does not re-read the port");
        } finally {
            release.countDown();
            compute.shutdown();
        }
        assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
    }

    @Test
    void customFactoryBuildsEveryEngineThread() throws Exception {
        final EngineTestSupport.SettingsBuilder builder = settings()
            .autoWorkers(1, 4, Duration.ofSeconds(30))
            .adaptive(new ComputeSettings.Adaptive(true, 1, 4, millis(100L), 5, 0.5d, 0.5d, 0.9d))
            .grace(millis(100L));
        final RecordingThreads threads = new RecordingThreads();
        final ComputeEngineImpl engine = new ComputeEngineImpl(
            builder.build(),
            new EngineTestSupport.FakeClock(),
            ComputeBudgetPort.fixed(4, "test"),
            threads
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            final String workerName = compute.submit(() -> Thread.currentThread().getName()).get(10L, TimeUnit.SECONDS);
            assertTrue(workerName.startsWith("spi-" + builder.prefix()), workerName);
        } finally {
            compute.shutdown();
        }
        assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));

        assertTrue(threads.asked("platform:" + builder.prefix()), "workers must come from the port");
        assertTrue(threads.asked("virtual:" + builder.prefix() + "-completion"), "completions must come from the port");
        assertTrue(threads.asked("virtual:" + builder.prefix() + "-virtual"), "lane tasks must come from the port");
        assertTrue(threads.asked("named:" + builder.prefix() + "-virtual-pump"), "the lane pump must come from the port");
        assertTrue(threads.asked("named:" + builder.prefix() + "-janitor"), "the janitor must come from the port");
        assertTrue(threads.asked("platform:" + builder.prefix() + "-adaptive"), "the adaptive timer must come from the port");
        assertTrue(threads.asked("platform:Tuff Compute Metrics"), "the metrics sampler must come from the port");
        assertTrue(threads.asked("named:" + builder.prefix() + "-shutdown"), "the shutdown thread must come from the port");
    }

    @Test
    void portOverloadsAssembleAStoppedScheduler() {
        final AsyncComputeScheduler compute = ComputeSchedulers.create(
            settings().fixedWorkers(1).noVirtualLane().build(),
            new EngineTestSupport.FakeClock(),
            ComputeBudgetPort.fixed(4, "test"),
            ComputeThreadFactoryPort.legacy()
        );
        assertEquals(AsyncComputeScheduler.SchedulerState.NEW, compute.state());
        compute.shutdown();
        assertEquals(AsyncComputeScheduler.SchedulerState.TERMINATED, compute.state());
    }

    /**
     * Delegating port that records what was asked of it and renames platform threads, so a test can
     * tell "the port was used" apart from "the legacy factory was used and happened to look the same".
     */
    private static final class RecordingThreads implements ComputeThreadFactoryPort {

        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final ComputeThreadFactoryPort delegate = ComputeThreadFactoryPort.legacy();

        @Override
        public ThreadFactory platform(final String namePrefix, final boolean daemon) {
            this.calls.add("platform:" + namePrefix);
            final ThreadFactory legacy = this.delegate.platform(namePrefix, daemon);
            return runnable -> {
                final Thread thread = legacy.newThread(runnable);
                thread.setName("spi-" + thread.getName());
                return thread;
            };
        }

        @Override
        public ThreadFactory virtual(final String namePrefix) {
            this.calls.add("virtual:" + namePrefix);
            return this.delegate.virtual(namePrefix);
        }

        @Override
        public ThreadFactory named(final String name, final boolean daemon) {
            this.calls.add("named:" + name);
            return this.delegate.named(name, daemon);
        }

        boolean asked(final String call) {
            return this.calls.contains(call);
        }
    }
}
