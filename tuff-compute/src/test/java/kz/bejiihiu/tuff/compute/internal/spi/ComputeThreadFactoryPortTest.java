package kz.bejiihiu.tuff.compute.internal.spi;

import java.util.concurrent.ThreadFactory;
import kz.bejiihiu.tuff.compute.internal.util.ThreadFactories;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The default port has to be a transparent stand-in for the factories the engine used before the port
 * existed: same names, same daemon flags, same virtual/platform split.
 */
class ComputeThreadFactoryPortTest {

    private static final Runnable NOOP = () -> {
    };

    @Test
    void legacyPlatformKeepsTheCounterSchemeAndDaemonFlag() {
        final ThreadFactory actual = ComputeThreadFactoryPort.legacy().platform("tuff-port", true);
        final Thread first = actual.newThread(NOOP);
        final Thread second = actual.newThread(NOOP);
        assertEquals("tuff-port-1", first.getName());
        assertEquals("tuff-port-2", second.getName());
        assertTrue(first.isDaemon());
        assertFalse(first.isVirtual());

        // the legacy port is the old factory, not a copy of it: a byte-for-byte comparison
        assertEquals(ThreadFactories.platform("tuff-port", true).newThread(NOOP).getName(), first.getName());
        assertFalse(ComputeThreadFactoryPort.legacy().platform("tuff-port", false).newThread(NOOP).isDaemon());
    }

    @Test
    void legacyVirtualKeepsTheCounterSchemeAndStaysVirtual() {
        final Thread thread = ComputeThreadFactoryPort.legacy().virtual("tuff-port-v").newThread(NOOP);
        assertEquals("tuff-port-v-1", thread.getName());
        assertTrue(thread.isVirtual());
        assertTrue(thread.isDaemon());
    }

    @Test
    void legacyNamedKeepsTheExactServiceThreadName() {
        final Thread thread = ComputeThreadFactoryPort.legacy().named("tuff-port-janitor", true).newThread(NOOP);
        assertEquals("tuff-port-janitor", thread.getName());
        assertTrue(thread.isDaemon());

        assertEquals(
            ThreadFactories.named("tuff-port-shutdown", true).newThread(NOOP).getName(),
            ComputeThreadFactoryPort.legacy().named("tuff-port-shutdown", true).newThread(NOOP).getName()
        );
    }

    @Test
    void blankNamesAreRejectedByTheLegacyPort() {
        assertThrows(IllegalArgumentException.class,
            () -> ComputeThreadFactoryPort.legacy().platform(" ", true));
        assertThrows(IllegalArgumentException.class,
            () -> ComputeThreadFactoryPort.legacy().virtual(""));
        assertThrows(IllegalArgumentException.class,
            () -> ComputeThreadFactoryPort.legacy().named("\t", true));
    }
}
