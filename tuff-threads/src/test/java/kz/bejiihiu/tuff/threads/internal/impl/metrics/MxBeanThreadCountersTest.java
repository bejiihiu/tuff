package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MxBeanThreadCountersTest {

    @Test
    void readsTheRealJvmCounters() {
        final MxBeanThreadCounters counters = new MxBeanThreadCounters();

        assertTrue(counters.startedTotal() > 0L);
        assertTrue(counters.liveThreads() > 0L);
        final double load = counters.processCpuLoad();
        assertTrue(load == -1.0d || load >= 0.0d && load <= 1.0d, "process load out of range: " + load);

        final long cpu = counters.cpuTimeNanos(Thread.currentThread().threadId());
        if (counters.cpuTimeSupported()) {
            assertTrue(cpu > 0L, "a running thread must have spent some CPU time");
        } else {
            // a runtime without CPU accounting is a legal configuration, not a failure
            assertEquals(-1L, cpu);
        }
        assertEquals(-1L, counters.cpuTimeNanos(-1L));
    }
}
