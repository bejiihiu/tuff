package kz.bejiihiu.tuff.compute.internal.metrics;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.ThroughputMeter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ThroughputMeterTest {

    private static final double TOLERANCE = 1.0e-9d;

    @Test
    void reportsTheRateOfTheLastInterval() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ThroughputMeter meter = new ThroughputMeter(clock, Duration.ofSeconds(1));
        meter.sample(0L);

        clock.advance(Duration.ofSeconds(1));
        meter.sample(100L);

        assertEquals(100.0d, meter.perSecond(), TOLERANCE);
        assertEquals(100.0d, meter.lastSecondPerSecond(), TOLERANCE);
        assertEquals(100.0d, meter.ewmaPerSecond(), TOLERANCE);
    }

    @Test
    void idleIntervalDropsTheRateToZero() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ThroughputMeter meter = new ThroughputMeter(clock, Duration.ofSeconds(1));
        meter.sample(0L);
        clock.advance(Duration.ofSeconds(1));
        meter.sample(100L);

        clock.advance(Duration.ofSeconds(1));
        meter.sample(100L);

        assertEquals(0.0d, meter.perSecond(), TOLERANCE);
        assertEquals(0.0d, meter.lastSecondPerSecond(), TOLERANCE);
        assertEquals(0.0d, meter.ewmaPerSecond(), TOLERANCE);
    }

    @Test
    void windowedRateAndEwmaUseDifferentSmoothing() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ThroughputMeter meter = new ThroughputMeter(clock, Duration.ofSeconds(4));
        meter.sample(0L);
        clock.advance(Duration.ofSeconds(1));
        meter.sample(100L);
        clock.advance(Duration.ofSeconds(1));
        meter.sample(100L);

        assertEquals(50.0d, meter.perSecond(), TOLERANCE);
        assertEquals(18.75d, meter.ewmaPerSecond(), TOLERANCE);
    }

    @Test
    void firstSampleOnlyEstablishesABaseline() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ThroughputMeter meter = new ThroughputMeter(clock, Duration.ofSeconds(1));

        meter.sample(42L);

        assertEquals(0.0d, meter.perSecond(), TOLERANCE);
        assertEquals(0.0d, meter.lastSecondPerSecond(), TOLERANCE);
        assertEquals(0.0d, meter.ewmaPerSecond(), TOLERANCE);
    }

    @Test
    void resetForgetsEverySample() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ThroughputMeter meter = new ThroughputMeter(clock, Duration.ofSeconds(1));
        meter.sample(0L);
        clock.advance(Duration.ofSeconds(1));
        meter.sample(100L);

        meter.reset();

        assertEquals(0.0d, meter.perSecond(), TOLERANCE);
        assertEquals(0.0d, meter.ewmaPerSecond(), TOLERANCE);
        meter.sample(500L);
        clock.advance(Duration.ofSeconds(1));
        meter.sample(600L);
        assertEquals(100.0d, meter.lastSecondPerSecond(), TOLERANCE);
    }

    @Test
    void repeatedSampleWithoutElapsedTimeDoesNotDivideByZero() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ThroughputMeter meter = new ThroughputMeter(clock, Duration.ofSeconds(1));
        meter.sample(0L);
        meter.sample(10L);

        assertEquals(0.0d, meter.perSecond(), TOLERANCE);
        assertEquals(0.0d, meter.lastSecondPerSecond(), TOLERANCE);
    }
}
