package kz.bejiihiu.tuff.threads.internal.impl.pools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the promises {@code ExecutionPoolRegistry} makes about identity, reload, refusal and snapshots.
 */
class ExecutionPoolRegistryImplTest {

    private final ExecutionPoolRegistryImpl registry = new ExecutionPoolRegistryImpl();

    private static List<PoolSpec> canonicalSpecs() {
        return List.of(
            PoolSpec.observeOnly(
                PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "region ticks"
            ),
            new PoolSpec(
                PoolId.COMPUTE, PoolKind.COMPUTE, "compute", "background compute",
                2, -1, CoverageLevel.L2, ResourceBudget.elastic(0.5d, 0)
            ),
            new PoolSpec(
                PoolId.COMPUTE_VIRTUAL, PoolKind.VIRTUAL, "compute", "virtual compute lane",
                1, -1, CoverageLevel.L2, ResourceBudget.elastic(0.1d, 0)
            ),
            new PoolSpec(
                PoolId.CHUNK_WORKER, PoolKind.CHUNK, "chunk-system", "chunk generation",
                4, 16, CoverageLevel.L2, ResourceBudget.defaults()
            ),
            new PoolSpec(
                PoolId.CHUNK_IO, PoolKind.IO, "chunk-system", "chunk file io",
                1, 4, CoverageLevel.L2, ResourceBudget.defaults()
            ),
            PoolSpec.observeOnly(PoolId.NETWORK, PoolKind.NETWORK, "netty", "network event loops"),
            PoolSpec.observeOnly(PoolId.SERVICE, PoolKind.SERVICE, "server", "service timers")
        );
    }

    @Test
    void registersCanonicalPoolsAndFiltersByKind() {
        for (final PoolSpec spec : canonicalSpecs()) {
            assertEquals(spec, this.registry.register(spec));
        }

        final List<PoolId> expected = List.of(
            PoolId.CHUNK_IO,
            PoolId.CHUNK_WORKER,
            PoolId.COMPUTE,
            PoolId.COMPUTE_VIRTUAL,
            PoolId.NETWORK,
            PoolId.REGION_EXECUTION,
            PoolId.SERVICE
        );
        assertEquals(expected, new ArrayList<>(this.registry.ids()));
        assertEquals(7, this.registry.specs().size());

        assertEquals(List.of(PoolId.COMPUTE), idsOf(this.registry.specs(PoolKind.COMPUTE)));
        assertEquals(List.of(PoolId.COMPUTE_VIRTUAL), idsOf(this.registry.specs(PoolKind.VIRTUAL)));
        assertEquals(List.of(PoolId.CHUNK_WORKER), idsOf(this.registry.specs(PoolKind.CHUNK)));
        assertEquals(List.of(PoolId.CHUNK_IO), idsOf(this.registry.specs(PoolKind.IO)));
        assertEquals(List.of(PoolId.REGION_EXECUTION), idsOf(this.registry.specs(PoolKind.REGION_EXECUTION)));
        assertEquals(List.of(PoolId.NETWORK), idsOf(this.registry.specs(PoolKind.NETWORK)));
        assertEquals(List.of(PoolId.SERVICE), idsOf(this.registry.specs(PoolKind.SERVICE)));

        assertTrue(this.registry.contains(PoolId.COMPUTE));
        assertEquals(PoolId.COMPUTE, this.registry.spec(PoolId.COMPUTE).orElseThrow().id());
        assertFalse(this.registry.contains(PoolId.of("unknown")));
        assertTrue(this.registry.spec(PoolId.of("unknown")).isEmpty());
    }

    @Test
    void duplicateRegistrationReplacesTheSameKind() {
        final PoolId id = PoolId.of("reloadable");
        final PoolSpec first = PoolSpec.observeOnly(id, PoolKind.COMPUTE, "compute", "observed compute");
        // an observe-only spec cannot be raised in place (min 0), so a reload registers a fresh L2 spec
        final PoolSpec upgraded = new PoolSpec(
            id, PoolKind.COMPUTE, "compute", "managed compute",
            1, -1, CoverageLevel.L2, ResourceBudget.defaults()
        );

        assertEquals(first, this.registry.register(first));
        assertEquals(first, this.registry.register(first));
        assertEquals(1, this.registry.specs().size());

        assertEquals(upgraded, this.registry.register(upgraded));
        assertEquals(upgraded, this.registry.spec(id).orElseThrow());
        assertEquals(1, this.registry.specs().size());
    }

    @Test
    void changingKindIsRefused() {
        final PoolId id = PoolId.of("not-a-chameleon");
        this.registry.register(PoolSpec.observeOnly(id, PoolKind.COMPUTE, "compute", "compute pool"));

        final PoolSpec otherKind = PoolSpec.observeOnly(id, PoolKind.IO, "chunk-system", "io pool");
        final IllegalStateException error =
            assertThrows(IllegalStateException.class, () -> this.registry.register(otherKind));

        assertTrue(error.getMessage().contains(id.value()), error.getMessage());
        assertTrue(error.getMessage().contains("COMPUTE"), error.getMessage());
        assertTrue(error.getMessage().contains("IO"), error.getMessage());
    }

    @Test
    void l0IsRefusedWithAnExplanation() {
        final PoolSpec outside = new PoolSpec(
            PoolId.of("outside"), PoolKind.SERVICE, "test", "jdk owned pool",
            0, -1, CoverageLevel.L0, ResourceBudget.defaults()
        );

        final IllegalArgumentException error =
            assertThrows(IllegalArgumentException.class, () -> this.registry.register(outside));

        assertTrue(error.getMessage().contains("L0"), error.getMessage());
        assertTrue(this.registry.specs().isEmpty());
    }

    @Test
    void snapshotsAreImmutable() {
        canonicalSpecs().forEach(this.registry::register);

        assertThrows(UnsupportedOperationException.class, () -> this.registry.ids().clear());
        assertThrows(UnsupportedOperationException.class, () -> this.registry.specs().clear());
        assertThrows(UnsupportedOperationException.class, () -> this.registry.specs(PoolKind.COMPUTE).clear());
    }

    @Test
    void nullsAreRefused() {
        assertThrows(NullPointerException.class, () -> this.registry.register(null));
        assertThrows(NullPointerException.class, () -> this.registry.unregister(null));
        assertThrows(NullPointerException.class, () -> this.registry.spec(null));
        assertThrows(NullPointerException.class, () -> this.registry.contains(null));
        assertThrows(NullPointerException.class, () -> this.registry.specs(null));
    }

    @Test
    void stopRefusesRegistrationsButKeepsReadsAndRemovals() {
        final PoolSpec spec = PoolSpec.observeOnly(PoolId.SERVICE, PoolKind.SERVICE, "server", "services");
        this.registry.register(spec);

        this.registry.markStopped();

        assertTrue(this.registry.isStopped());
        final PoolSpec late = PoolSpec.observeOnly(PoolId.NETWORK, PoolKind.NETWORK, "netty", "late pool");
        final IllegalStateException error =
            assertThrows(IllegalStateException.class, () -> this.registry.register(late));
        assertTrue(error.getMessage().contains("stopped"), error.getMessage());

        assertTrue(this.registry.contains(PoolId.SERVICE));
        assertEquals(1, this.registry.specs().size());
        assertTrue(this.registry.unregister(PoolId.SERVICE));
        assertTrue(this.registry.ids().isEmpty());
        assertFalse(this.registry.unregister(PoolId.SERVICE));
    }

    @Test
    void snapshotsStayConsistentUnderConcurrentRegistration() throws Exception {
        final int workers = 8;
        final int perWorker = 40;
        canonicalSpecs().forEach(this.registry::register);

        final ExecutorService executor = Executors.newFixedThreadPool(workers + 1);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final List<Future<?>> futures = new ArrayList<>();

            for (int worker = 0; worker < workers; worker++) {
                final int index = worker;
                futures.add(executor.submit(() -> {
                    awaitQuietly(start);
                    for (int i = 0; i < perWorker; i++) {
                        final PoolId id = PoolId.of("load-" + index + "-" + i);
                        this.registry.register(
                            PoolSpec.observeOnly(id, PoolKind.SERVICE, "test", "concurrent pool")
                        );
                    }
                }));
            }
            futures.add(executor.submit(() -> {
                awaitQuietly(start);
                while (running.get() && failure.get() == null) {
                    try {
                        this.assertSnapshotIsInternallyConsistent();
                    } catch (final Throwable error) {
                        failure.compareAndSet(null, error);
                    }
                }
            }));

            start.countDown();
            for (int worker = 0; worker < workers; worker++) {
                futures.get(worker).get(20, TimeUnit.SECONDS);
            }
            running.set(false);
            futures.get(workers).get(20, TimeUnit.SECONDS);

            assertNull(failure.get(), () -> "snapshot was inconsistent: " + failure.get());
        } finally {
            executor.shutdownNow();
        }

        assertEquals(7 + workers * perWorker, this.registry.ids().size());
        assertTrue(this.registry.contains(PoolId.of("load-0-0")));
        assertTrue(this.registry.contains(PoolId.of("load-7-39")));
    }

    @Test
    void duplicateRegistrationIsIdempotentUnderConcurrency() throws Exception {
        final int workers = 8;
        final int perWorker = 25;
        final PoolSpec spec = new PoolSpec(
            PoolId.COMPUTE, PoolKind.COMPUTE, "compute", "background compute",
            1, -1, CoverageLevel.L2, ResourceBudget.defaults()
        );

        final ExecutorService executor = Executors.newFixedThreadPool(workers);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < workers; worker++) {
                futures.add(executor.submit(() -> {
                    awaitQuietly(start);
                    for (int i = 0; i < perWorker; i++) {
                        assertEquals(spec, this.registry.register(spec));
                    }
                }));
            }
            start.countDown();
            for (final Future<?> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(List.of(spec), this.registry.specs());
    }

    private static List<PoolId> idsOf(final List<PoolSpec> specs) {
        final List<PoolId> ids = new ArrayList<>(specs.size());
        for (final PoolSpec spec : specs) {
            ids.add(spec.id());
        }
        return ids;
    }

    private static void awaitQuietly(final CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the test start", interrupted);
        }
    }

    private void assertSnapshotIsInternallyConsistent() {
        final Set<PoolId> ids = this.registry.ids();
        final List<PoolId> ordered = new ArrayList<>(ids);
        final List<PoolId> sorted = new ArrayList<>(ordered);
        Collections.sort(sorted);
        assertEquals(sorted, ordered, "ids() is not in natural order");
        assertEquals(ids.size(), new HashSet<>(ordered).size(), "ids() contains a duplicate");

        final List<PoolSpec> specs = this.registry.specs();
        final List<PoolId> specIds = idsOf(specs);
        final List<PoolId> sortedSpecIds = new ArrayList<>(specIds);
        Collections.sort(sortedSpecIds);
        assertEquals(sortedSpecIds, specIds, "specs() is not in id order");
        assertEquals(specIds.size(), new HashSet<>(specIds).size(), "specs() contains a duplicate");

        for (final PoolSpec spec : specs) {
            assertEquals(spec, this.registry.spec(spec.id()).orElseThrow());
        }
    }
}
