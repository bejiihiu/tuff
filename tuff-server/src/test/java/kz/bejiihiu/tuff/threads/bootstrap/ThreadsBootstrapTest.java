package kz.bejiihiu.tuff.threads.bootstrap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.config.ThreadsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadsBootstrapTest {

    @TempDir
    Path directory;

    @Test
    void anEnabledConfigurationRegistersTheObserveOnlyPools() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: true
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();

        bootstrap.startWith(new ThreadsConfig(file));

        final ThreadsStatus status = bootstrap.currentStatus();
        assertEquals(ThreadsStatus.State.RUNNING, status.state());
        assertTrue(bootstrap.regionPoolRegistered(), "the region pool must be registered by the runtime");
        final ResourceSnapshot snapshot = status.snapshot();
        assertNotNull(snapshot, "a running layer must produce a snapshot");
        final PoolSnapshot region = snapshot.pool(PoolId.REGION_EXECUTION).orElseThrow();
        assertEquals(CoverageLevel.L1, region.coverage(), "the default is observation only");
        assertEquals(0, region.spec().minThreads());

        bootstrap.shutdown();
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, bootstrap.currentStatus().state());
        assertFalse(bootstrap.regionPoolRegistered());
    }

    @Test
    void aDisabledConfigurationCreatesNothingAtAll() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: false
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();

        bootstrap.startWith(new ThreadsConfig(file));

        final ThreadsStatus status = bootstrap.currentStatus();
        assertEquals(ThreadsStatus.State.DISABLED, status.state());
        assertNull(status.snapshot());
        assertFalse(bootstrap.regionPoolRegistered(), "a disabled layer must not register a pool");

        bootstrap.shutdown();
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, bootstrap.currentStatus().state());
    }

    @Test
    void aSecondStartIsANoOp() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: true
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        final ThreadsConfig config = new ThreadsConfig(file);

        bootstrap.startWith(config);
        bootstrap.startWith(config);

        assertEquals(ThreadsStatus.State.RUNNING, bootstrap.currentStatus().state());
        bootstrap.shutdown();
    }

    @Test
    void aMissingConfigurationStillStartsWithTheDefaults() {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();

        bootstrap.startWith(new ThreadsConfig(this.directory.resolve("absent.yml")));

        assertEquals(ThreadsStatus.State.RUNNING, bootstrap.currentStatus().state());
        assertTrue(bootstrap.regionPoolRegistered());
        bootstrap.shutdown();
    }

    @Test
    void threadsListedByAProbeAreNotReportedAsUnknown() {
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 1, 0, 1, 1, 0, 1, 0.0d, 0.0d, -1.0d,
            List.of(),
            List.of(new ThreadSnapshot(7L, "Folia Region Scheduler Thread #1", PoolId.REGION_EXECUTION,
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L)),
            Set.of(),
            Optional.empty()
        );
        final List<ThreadSnapshot> unregistered = List.of(
            new ThreadSnapshot(7L, "Folia Region Scheduler Thread #1", PoolId.of("unregistered"),
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(9L, "Netty Epoll Server IO #0", PoolId.of("unregistered"),
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L)
        );

        assertEquals(1, ThreadsBootstrap.countUnknown(unregistered, snapshot));
    }

    private Path write(final String contents) throws IOException {
        final Path file = this.directory.resolve("tuff.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        return file;
    }
}
