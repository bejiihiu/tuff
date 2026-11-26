package kz.bejiihiu.tuff.threads.internal.impl.pools;

import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers attach and detach rules: a probe needs a registered pool, replacement is idempotent, detach is
 * a plain remove, and a stopped registry takes no new probes but still lets owners clean up.
 */
class PoolProbeRegistryImplTest {

    private final ExecutionPoolRegistryImpl pools = new ExecutionPoolRegistryImpl();
    private final PoolProbeRegistryImpl probes = new PoolProbeRegistryImpl(this.pools);

    @Test
    void attachRequiresARegisteredPool() {
        final FakeProbe probe = new FakeProbe(PoolId.of("ghost"));
        final IllegalStateException error = assertThrows(IllegalStateException.class, () -> this.probes.attach(probe));

        assertTrue(error.getMessage().contains("not registered"), error.getMessage());
        assertThrows(NullPointerException.class, () -> this.probes.attach(null));
    }

    @Test
    void attachReplacesAndDetachIsIdempotent() {
        final PoolId id = PoolId.of("observed");
        this.pools.register(PoolSpec.observeOnly(id, PoolKind.SERVICE, "test", "observed pool"));
        final FakeProbe first = new FakeProbe(id);
        final FakeProbe second = new FakeProbe(id);

        this.probes.attach(first);
        this.probes.attach(second);

        assertEquals(1, this.probes.attached().size());
        assertSame(second, this.probes.attached().getFirst());
        assertTrue(this.probes.detach(id));
        assertFalse(this.probes.detach(id));
        assertTrue(this.probes.attached().isEmpty());
        assertTrue(this.pools.contains(id), "detaching a probe must not unregister the pool");
    }

    @Test
    void unregisteringAPoolDropsItsProbe() {
        final PoolId id = PoolId.of("observed");
        this.pools.register(PoolSpec.observeOnly(id, PoolKind.SERVICE, "test", "observed pool"));
        this.probes.attach(new FakeProbe(id));

        this.pools.unregister(id);

        assertTrue(this.probes.attached().isEmpty());
        assertFalse(this.probes.detach(id));
    }

    @Test
    void attachedPreservesPoolOrderAndIsImmutable() {
        this.pools.register(PoolSpec.observeOnly(PoolId.NETWORK, PoolKind.NETWORK, "netty", "network"));
        this.pools.register(PoolSpec.observeOnly(PoolId.COMPUTE, PoolKind.COMPUTE, "compute", "compute"));
        this.probes.attach(new FakeProbe(PoolId.NETWORK));
        this.probes.attach(new FakeProbe(PoolId.COMPUTE));

        final List<PoolId> ids = new ArrayList<>();
        this.probes.attached().forEach(probe -> ids.add(probe.pool()));
        assertEquals(List.of(PoolId.COMPUTE, PoolId.NETWORK), ids);
        assertThrows(UnsupportedOperationException.class, () -> this.probes.attached().clear());
    }

    @Test
    void stopRefusesAttachmentsButKeepsDetach() {
        final PoolId id = PoolId.of("observed");
        this.pools.register(PoolSpec.observeOnly(id, PoolKind.SERVICE, "test", "observed pool"));
        this.probes.attach(new FakeProbe(id));

        this.probes.markStopped();

        assertTrue(this.probes.isStopped());
        final IllegalStateException error = assertThrows(
            IllegalStateException.class, () -> this.probes.attach(new FakeProbe(id))
        );
        assertTrue(error.getMessage().contains("stopped"), error.getMessage());
        assertTrue(this.probes.detach(id));
        assertFalse(this.probes.detach(id));
    }

    private static final class FakeProbe implements PoolProbe {

        private final PoolId pool;

        private FakeProbe(final PoolId pool) {
            this.pool = pool;
        }

        @Override
        public PoolId pool() {
            return this.pool;
        }

        @Override
        public PoolObservation observe() {
            return EmptyObservation.INSTANCE;
        }
    }

    private enum EmptyObservation implements PoolObservation {

        INSTANCE;

        @Override
        public int aliveThreads() {
            return 0;
        }

        @Override
        public int coreThreads() {
            return 0;
        }

        @Override
        public int activeThreads() {
            return -1;
        }

        @Override
        public int idleThreads() {
            return -1;
        }

        @Override
        public long queuedTasks() {
            return -1L;
        }

        @Override
        public long queueCapacity() {
            return -1L;
        }

        @Override
        public List<Thread> threads() {
            return List.of();
        }
    }
}
