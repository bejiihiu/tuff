package kz.bejiihiu.tuff.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Deterministic load shapes driven through the server console.
 *
 * <p>The stand has no client bots: a run is one server process plus commands sent to its console, so
 * every profile has to be produced by real server work (tickets, entities, region lifecycle) rather
 * than by simulated numbers. The shapes match the methodology profiles:
 *
 * <ul>
 *     <li>{@code steady} - a fixed set of force-loaded areas with a fixed entity count, the baseline
 *     non-inferiority profile;</li>
 *     <li>{@code churn} - forced areas are dropped and re-added and a slice of entities is killed and
 *     respawned, which makes regions appear and disappear;</li>
 *     <li>{@code burst} - a large entity spike followed by a trough, which is the R3 spawn/retire
 *     trigger;</li>
 *     <li>{@code idle-compute} - few regions and no entity work. The compute lane cannot be loaded
 *     without a plugin, so this profile honestly measures the idle server and the report says so.</li>
 * </ul>
 *
 * <p>Every command is a vanilla/Bukkit command that the real server executes; nothing here writes
 * measurements. Profile actions are keyed off elapsed run time, never off wall-clock time of day, so
 * two runs of the same profile send the same commands in the same order.
 */
enum LoadProfile {

    /** Fixed tickets and a fixed entity count. */
    STEADY("steady"),

    /** Tickets and entities cycle, so regions are created and destroyed. */
    CHURN("churn"),

    /** Entity spikes followed by troughs. */
    BURST("burst"),

    /** Few regions, no entity work; the compute lane stays unloaded. */
    IDLE_COMPUTE("idle-compute");

    private final String cli;

    LoadProfile(final String cli) {
        this.cli = cli;
    }

    /**
     * Returns the value accepted on the command line.
     *
     * @return the CLI name
     */
    String cli() {
        return this.cli;
    }

    /**
     * Parses a profile name.
     *
     * @param raw CLI name, case insensitive
     * @return the profile
     * @throws IllegalArgumentException if the name is unknown
     */
    static LoadProfile parse(final String raw) {
        final String normalized = raw.trim().toLowerCase(Locale.ROOT);
        for (final LoadProfile profile : values()) {
            if (profile.cli.equals(normalized)) {
                return profile;
            }
        }
        throw new IllegalArgumentException(
            "unknown profile '" + raw + "'; expected steady, churn, burst or idle-compute"
        );
    }

    /**
     * Returns the commands that build the starting state, sent once after the server is up.
     *
     * @param spec load shape
     * @return console commands without the leading slash
     */
    List<String> setup(final LoadSpec spec) {
        final List<String> commands = new ArrayList<>(16);
        commands.add("gamerule doMobSpawning false");
        commands.add("gamerule doDaylightCycle false");
        commands.add("gamerule doWeatherCycle false");
        commands.add("gamerule randomTickSpeed 0");
        commands.add("time set 6000");
        commands.add("kill @e[type=minecraft:" + spec.entityType() + ']');
        // a world copied from the template must not bring the template's own tickets with it
        commands.add("forceload remove all");
        final int areas = this == IDLE_COMPUTE ? 1 : spec.areas();
        for (int index = 0; index < areas; ++index) {
            commands.add(spec.forceLoadCommand(index));
        }
        if (this == STEADY) {
            for (int index = 0; index < areas; ++index) {
                commands.addAll(spec.summonCommands(index, spec.entitiesPerArea()));
            }
        }
        return List.copyOf(commands);
    }

    /**
     * Returns the commands due at one sample boundary.
     *
     * <p>The phase inside each cycle is derived from elapsed milliseconds, so the schedule is
     * reproducible and independent of how long an individual sample took.
     *
     * @param spec          load shape
     * @param elapsedMillis time since the measurement run started
     * @return commands to send before the next sample, possibly empty
     */
    List<String> periodic(final LoadSpec spec, final long elapsedMillis) {
        final long second = elapsedMillis / 1000L;
        return switch (this) {
            case STEADY -> List.of();
            case CHURN -> {
                if (second % 10L != 0L) {
                    yield List.of();
                }
                final int areaCount = Math.max(1, spec.areas());
                final int step = (int) ((second / 10L) % areaCount);
                final List<String> commands = new ArrayList<>(8);
                commands.add(spec.unforceLoadCommand(step));
                commands.addAll(spec.summonInAreaCommand((step + 1) % areaCount, Math.max(1, spec.entitiesPerArea() / 8)));
                if (second >= 30L) {
                    // the area dropped three steps ago comes back, so the region count oscillates
                    // instead of only falling
                    commands.add(spec.forceLoadCommand((step + areaCount - 3) % areaCount));
                }
                yield List.copyOf(commands);
            }
            case BURST -> {
                final long phase = second % 20L;
                if (phase == 0L) {
                    yield spec.summonCommands(0, spec.entitiesPerArea());
                }
                if (phase == 8L) {
                    yield List.of("kill @e[type=minecraft:" + spec.entityType() + ']');
                }
                yield List.of();
            }
            case IDLE_COMPUTE -> {
                if (second % 20L != 0L) {
                    yield List.of();
                }
                // a small ticket churn keeps the single region alive without adding real work
                final int area = (int) ((second / 20L) % Math.max(1, spec.areas()));
                yield List.of(spec.forceLoadCommand(area));
            }
        };
    }
}
