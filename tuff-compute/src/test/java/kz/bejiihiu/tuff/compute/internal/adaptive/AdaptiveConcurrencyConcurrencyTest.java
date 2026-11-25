package kz.bejiihiu.tuff.compute.internal.adaptive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.internal.adaptive.impl.AimdAdaptiveConcurrencyController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.automatic;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class AdaptiveConcurrencyConcurrencyTest {

    @Test
    void concurrentEvaluateRecordAndQueueStateKeepTheInvariants() throws Exception {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            settings(automatic(1, 16), true, 1, 16, 2, 0.5d, 0.5d, 0.8d),
            clock,
            4,
            AdaptiveTestSupport.PROCESSORS,
            null,
            OptionalDouble::empty);
        controller.start();

        final int iterations = 2_000;
        final ExecutorService executor = Executors.newFixedThreadPool(7);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            for (int worker = 0; worker < 3; worker++) {
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        final int limit = controller.evaluate();
                        assertTrue(limit >= controller.minLimit());
                        assertTrue(limit <= controller.maxLimit());
                    }
                }));
            }
            for (int worker = 0; worker < 3; worker++) {
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        controller.recordSample(i % 1_000_000L, 5_000_000L, i % 23 == 0);
                        controller.onQueueState(i % 7, 8192);
                        controller.snapshot();
                    }
                }));
            }
            futures.add(executor.submit(() -> {
                for (int i = 0; i < 500; i++) {
                    clock.advance(Duration.ofSeconds(1));
                    Thread.onSpinWait();
                }
            }));

            for (final Future<?> future : futures) {
                future.get();
            }
        } catch (final ExecutionException failure) {
            throw new AssertionError("concurrent access broke an invariant", failure.getCause());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        controller.stop();
        assertTrue(controller.currentLimit() >= controller.minLimit());
        assertTrue(controller.currentLimit() <= controller.maxLimit());
        assertTrue(controller.snapshot().samples() >= 0);
    }
}
