package kz.bejiihiu.tuff.compute.internal.adaptive;

import java.time.Duration;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.adaptive.impl.AimdAdaptiveConcurrencyController;

/**
 * Shared fixtures for the adaptive controller tests.
 */
final class AdaptiveTestSupport {

    static final int PROCESSORS = 8;
    static final Duration WINDOW = Duration.ofSeconds(1);
    static final long MILLIS = TimeUnit.MILLISECONDS.toNanos(1L);

    private AdaptiveTestSupport() {
    }

    /**
     * Automatic worker sizing with the usual keep alive, so the controller may move the pool.
     *
     * @param minWorkers floor of the worker configuration, at least {@code 1}
     * @param maxWorkers ceiling of the worker configuration, at least {@code minWorkers}
     * @return the worker section
     */
    static ComputeSettings.Workers automatic(final int minWorkers, final int maxWorkers) {
        return new ComputeSettings.Workers(
            -1, minWorkers, maxWorkers, Duration.ofSeconds(30), "adaptive-test-worker");
    }

    /**
     * Explicit fixed worker count, the case where adaptive sizing must stay out of the way.
     *
     * @param threads fixed worker count, at least {@code 1}
     * @return the worker section
     */
    static ComputeSettings.Workers fixed(final int threads) {
        return new ComputeSettings.Workers(
            threads, 1, threads, Duration.ofSeconds(30), "adaptive-test-worker");
    }

    /**
     * Builds full settings around one adaptive section.
     *
     * @param workers           worker section
     * @param enabled           whether adaptive concurrency is enabled
     * @param minLimit          adaptive minimum
     * @param maxLimit          adaptive maximum, {@code -1} for the worker ceiling
     * @param minSamples        samples required per window
     * @param increaseStep      additive growth step fraction
     * @param decreaseFactor    multiplicative decrease factor
     * @param targetUtilization target utilisation
     * @return complete settings
     */
    static ComputeSettings settings(
        final ComputeSettings.Workers workers,
        final boolean enabled,
        final int minLimit,
        final int maxLimit,
        final int minSamples,
        final double increaseStep,
        final double decreaseFactor,
        final double targetUtilization
    ) {
        return new ComputeSettings(
            true,
            workers,
            ComputeSettings.QueueSettings.defaults(),
            ComputeSettings.Scheduling.defaults(),
            ComputeSettings.Consumers.defaults(),
            ComputeSettings.Metrics.defaults(),
            new ComputeSettings.Adaptive(
                enabled,
                minLimit,
                maxLimit,
                WINDOW,
                minSamples,
                increaseStep,
                decreaseFactor,
                targetUtilization),
            Duration.ofSeconds(30));
    }

    /**
     * Creates a controller wired to a fake clock and no CPU signal.
     *
     * @param settings     full settings
     * @param clock        fake clock
     * @param initialLimit starting concurrency
     * @return the controller, already started
     */
    static AimdAdaptiveConcurrencyController controller(
        final ComputeSettings settings,
        final TestComputeClock clock,
        final int initialLimit
    ) {
        final AimdAdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            settings, clock, initialLimit, PROCESSORS, null, OptionalDouble::empty);
        controller.start();
        return controller;
    }

    /**
     * Records {@code count} finished tasks of the same shape.
     *
     * @param controller controller to feed
     * @param count      number of samples
     * @param waitMillis queue wait in milliseconds
     * @param execMillis execution time in milliseconds
     */
    static void feed(
        final AimdAdaptiveConcurrencyController controller,
        final int count,
        final long waitMillis,
        final long execMillis
    ) {
        for (int i = 0; i < count; i++) {
            controller.recordSample(waitMillis * MILLIS, execMillis * MILLIS, false);
        }
    }
}
