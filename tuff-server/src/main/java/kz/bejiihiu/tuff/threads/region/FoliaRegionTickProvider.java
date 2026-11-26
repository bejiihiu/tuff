package kz.bejiihiu.tuff.threads.region;

import ca.spottedleaf.common.time.TickData;
import ca.spottedleaf.moonrise.common.util.TickThread;
import io.papermc.paper.threadedregions.TickRegionScheduler;
import io.papermc.paper.threadedregions.TickRegions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import org.jspecify.annotations.Nullable;

/**
 * Reads the tick reports of every live region from Folia's scheduler.
 *
 * <p>Only the two cheap windows are read: 5 s and 15 s. The 1 m, 5 m and 15 m reports copy and sort one
 * record per tick over thirty thousand ticks at the top end, which is milliseconds and a pile of garbage
 * per region - acceptable for a command that asks for it, not for a layer that samples. Per-region entity,
 * player and chunk counts come from {@code RegionStats}, which are acquire reads.
 *
 * <p>Two limits protect the server. Reads are throttled to one per second here, not only in the sampler:
 * a second command in the same second gets the previous list instead of a second walk over every region.
 * And a read from a region tick thread never takes the regioniser lock - it answers with the last list,
 * because the sampler can be triggered by a player running {@code /tuff threads}.
 *
 * <p>A region whose windows are empty (just created, just reactivated after a merge or split) is reported
 * with {@code -1} measurements instead of being dropped: the pool does tick it, there is just nothing to
 * average yet.
 */
public final class FoliaRegionTickProvider implements RegionTickProvider {

    /**
     * Shortest gap between two walks over the regions.
     */
    static final long MIN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1L);

    private final Supplier<? extends Iterable<ServerLevel>> worlds;
    private final LongSupplier nanoTime;
    private final Object lock = new Object();

    private volatile List<RegionReading> last = List.of();
    private long lastReadNanos = Long.MIN_VALUE;

    /**
     * Creates the provider bound to the live server.
     */
    public FoliaRegionTickProvider() {
        this(FoliaRegionTickProvider::levels, System::nanoTime);
    }

    /**
     * Creates a provider with explicit sources, for tests.
     *
     * @param worlds   source of the levels to walk
     * @param nanoTime time source for the read throttle
     * @throws NullPointerException if a source is {@code null}
     */
    FoliaRegionTickProvider(final Supplier<? extends Iterable<ServerLevel>> worlds, final LongSupplier nanoTime) {
        this.worlds = Objects.requireNonNull(worlds, "worlds");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /**
     * Returns the per-region readings of the current moment.
     *
     * @return an immutable list, possibly empty; never {@code null}
     */
    @Override
    public List<RegionReading> read() {
        if (TickThread.isTickThread()) {
            // the regioniser read lock is cheap from anywhere except a tick thread, and a command run
            // by a player would otherwise take it from inside the tick
            return this.last;
        }
        final long now = this.nanoTime.getAsLong();
        synchronized (this.lock) {
            if (this.lastReadNanos != Long.MIN_VALUE && now - this.lastReadNanos < MIN_INTERVAL_NANOS) {
                return this.last;
            }
            // the timestamp moves even when the read fails: a broken server must not be walked every call
            this.lastReadNanos = now;
            this.last = this.walk();
            return this.last;
        }
    }

    private List<RegionReading> walk() {
        final List<RegionReading> readings = new ArrayList<>();
        try {
            for (final ServerLevel world : this.worlds.get()) {
                if (world == null) {
                    continue;
                }
                final long now = System.nanoTime();
                world.regioniser.computeForAllRegions(region -> {
                    final @Nullable RegionReading reading = readingOf(region.getData(), now);
                    if (reading != null) {
                        readings.add(reading);
                    }
                });
            }
        } catch (RuntimeException | LinkageError _) {
            // a server that is starting or stopping has no readable regions; the summary says zero
            return List.copyOf(readings);
        }
        return List.copyOf(readings);
    }

    private static @Nullable RegionReading readingOf(
        final TickRegions.TickRegionData data,
        final long now
    ) {
        final TickRegionScheduler.RegionScheduleHandle handle = data.getRegionSchedulingHandle();
        if (handle == null) {
            return null;
        }
        // null when the window is empty: the region exists, but it has not ticked yet
        final TickData.@Nullable TickReportData report5s = handle.getTickReport5s(now);
        final TickData.@Nullable TickReportData report15s = handle.getTickReport15s(now);
        final TickRegions.RegionStats stats = data.getRegionStats();
        return new RegionReading(
            tps(report5s),
            tps(report15s),
            averageMspt(report5s),
            maximumMspt(report5s),
            averageMspt(report15s),
            maximumMspt(report15s),
            stats.getEntityCount(),
            stats.getPlayerCount(),
            stats.getChunkCount()
        );
    }

    private static double tps(final TickData.@Nullable TickReportData report) {
        return report == null ? -1.0d : report.tpsData().segmentAll().average();
    }

    private static double averageMspt(final TickData.@Nullable TickReportData report) {
        return report == null ? -1.0d : report.timePerTickData().segmentAll().average() / 1.0e6d;
    }

    private static double maximumMspt(final TickData.@Nullable TickReportData report) {
        return report == null ? -1.0d : report.timePerTickData().segmentAll().greatest() / 1.0e6d;
    }

    private static Iterable<ServerLevel> levels() {
        final @Nullable MinecraftServer server = MinecraftServer.getServer();
        return server == null ? List.of() : server.getAllLevels();
    }
}
