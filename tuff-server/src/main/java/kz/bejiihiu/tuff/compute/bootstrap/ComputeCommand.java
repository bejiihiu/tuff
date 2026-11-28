package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.jspecify.annotations.Nullable;

/**
 * Argument handling of the {@code /tuff} command, separated from Bukkit.
 *
 * <p>The parser is a pure function of the argument array: it never throws, never touches the server
 * and always returns something the command can act on. That keeps the command class down to
 * permission checks and message delivery, and lets the accepted grammar be tested on its own.
 *
 * <p>Accepted forms:
 * <ul>
 *     <li>{@code /tuff} and {@code /tuff compute} - engine summary;</li>
 *     <li>{@code /tuff compute consumers} - per consumer breakdown;</li>
 *     <li>{@code /tuff compute tasks [n]} - running tasks and the slowest recent completions;</li>
 *     <li>{@code /tuff compute reload} - re-read {@code config/tuff.yml};</li>
 *     <li>{@code /tuff compute jfr start|stop|dump} - Flight Recorder control;</li>
 *     <li>{@code /tuff threads} - pools, coverage and degraded reasons of the thread layer;</li>
 *     <li>{@code /tuff scheduler} - active region scheduler, Kairos fund and wait queue;</li>
 *     <li>{@code /tuff scheduler leases [n]} - worst recent permit waits;</li>
 *     <li>{@code /tuff scheduler budget} - budget ledger, usable cores and degraded reasons;</li>
 *     <li>{@code /tuff scheduler reset} - zero the scheduler's A/B counters;</li>
 *     <li>{@code /tuff scheduler help} - the scheduler grammar;</li>
 *     <li>{@code /tuff status} and {@code /tuff reload} - shorthands for the two commands above.</li>
 * </ul>
 */
public final class ComputeCommand {

    /**
     * Usage line shown on argument errors and in {@code /help}.
     */
    public static final String USAGE =
        "/tuff [compute] [consumers|tasks [n]|reload|jfr <start|stop|dump>] | /tuff threads | "
            + "/tuff scheduler [leases [n]|budget|reset|help]";

    /**
     * Usage line of the scheduler subcommand, shown by {@code /tuff scheduler help} and on a wrong
     * scheduler argument.
     */
    public static final String SCHEDULER_USAGE = "/tuff scheduler [leases [n]|budget|reset|help]";

    /**
     * Number of tasks {@code /tuff compute tasks} shows when no count is given.
     */
    public static final int DEFAULT_TASK_LIMIT = 10;

    /**
     * Upper bound for the optional task count.
     */
    public static final int MAX_TASK_LIMIT = 50;

    /**
     * Number of permit waits {@code /tuff scheduler leases} shows when no count is given.
     */
    public static final int DEFAULT_LEASE_LIMIT = 5;

    /**
     * Upper bound for the optional lease count.
     */
    public static final int MAX_LEASE_LIMIT = 20;

    private static final List<String> ROOT = List.of("compute", "status", "reload", "threads", "scheduler");
    private static final List<String> COMPUTE = List.of("consumers", "tasks", "reload", "jfr");
    private static final List<String> JFR = List.of("start", "stop", "dump");
    private static final List<String> SCHEDULER = List.of("leases", "budget", "reset", "help");

    private ComputeCommand() {
    }

    /**
     * Parses the raw arguments of one command invocation.
     *
     * @param args arguments as Bukkit passes them, never {@code null}
     * @return the action to run, or an error explaining what was wrong
     */
    public static Request parse(final String[] args) {
        if (args.length == 0) {
            return Request.of(Action.SUMMARY);
        }
        return switch (args[0].toLowerCase(Locale.ROOT)) {
            case "status" -> exactly(args, 1, Action.SUMMARY);
            case "reload" -> exactly(args, 1, Action.RELOAD);
            case "threads" -> exactly(args, 1, Action.THREADS);
            case "scheduler" -> parseScheduler(args);
            case "compute" -> parseCompute(args);
            default -> Request.error("Unknown subcommand '" + args[0] + "'. Usage: " + USAGE);
        };
    }

    private static Request parseScheduler(final String[] args) {
        if (args.length == 1) {
            return Request.of(Action.SCHEDULER);
        }
        return switch (args[1].toLowerCase(Locale.ROOT)) {
            case "leases" -> parseLeases(args);
            case "budget" -> exactly(args, 2, Action.SCHEDULER_BUDGET, SCHEDULER_USAGE);
            case "reset" -> exactly(args, 2, Action.SCHEDULER_RESET, SCHEDULER_USAGE);
            case "help" -> exactly(args, 2, Action.SCHEDULER_HELP, SCHEDULER_USAGE);
            default -> Request.error(
                "Unknown scheduler subcommand '" + args[1] + "'. Usage: " + SCHEDULER_USAGE
            );
        };
    }

    private static Request parseLeases(final String[] args) {
        if (args.length == 2) {
            return Request.leases(DEFAULT_LEASE_LIMIT);
        }
        if (args.length > 3) {
            return Request.error("Usage: /tuff scheduler leases [n]");
        }
        final int requested;
        try {
            requested = Integer.parseInt(args[2]);
        } catch (NumberFormatException _) {
            return Request.error("'" + args[2] + "' must be a number between 1 and " + MAX_LEASE_LIMIT);
        }
        return Request.leases(Math.clamp(requested, 1, MAX_LEASE_LIMIT));
    }

    private static Request parseCompute(final String[] args) {
        if (args.length == 1) {
            return Request.of(Action.SUMMARY);
        }
        return switch (args[1].toLowerCase(Locale.ROOT)) {
            case "consumers" -> exactly(args, 2, Action.CONSUMERS);
            case "tasks" -> parseTasks(args);
            case "reload" -> exactly(args, 2, Action.RELOAD);
            case "jfr" -> parseJfr(args);
            default -> Request.error("'" + args[1] + "' must be one of: consumers, tasks, reload, jfr");
        };
    }

    private static Request parseTasks(final String[] args) {
        if (args.length == 2) {
            return Request.tasks(DEFAULT_TASK_LIMIT);
        }
        if (args.length > 3) {
            return Request.error("Usage: /tuff compute tasks [n]");
        }
        final int requested;
        try {
            requested = Integer.parseInt(args[2]);
        } catch (NumberFormatException _) {
            return Request.error("'" + args[2] + "' must be a number between 1 and " + MAX_TASK_LIMIT);
        }
        return Request.tasks(Math.clamp(requested, 1, MAX_TASK_LIMIT));
    }

    /**
     * Suggests the words that are valid at the position the sender is typing.
     *
     * @param args arguments as Bukkit passes them, including the incomplete last one
     * @return matching completions, possibly empty
     */
    public static List<String> completions(final String[] args) {
        if (args.length == 0) {
            return List.of();
        }
        final int index = args.length - 1;
        final boolean compute = args[0].equalsIgnoreCase("compute");
        final boolean scheduler = args[0].equalsIgnoreCase("scheduler");
        if (index >= 1 && !compute && !scheduler) {
            return List.of();
        }
        if (index >= 2 && !(compute && args[1].equalsIgnoreCase("jfr"))) {
            // tasks and leases take a number, the other subcommands take nothing: no word to suggest
            return List.of();
        }
        final List<String> candidates = switch (index) {
            case 0 -> ROOT;
            case 1 -> compute ? COMPUTE : SCHEDULER;
            default -> JFR;
        };
        return filter(candidates, args[index]);
    }

    private static Request parseJfr(final String[] args) {
        if (args.length == 2) {
            return Request.error("Usage: /tuff compute jfr <start|stop|dump>");
        }
        if (args.length > 3) {
            return Request.error("Usage: /tuff compute jfr <start|stop|dump>");
        }
        return switch (args[2].toLowerCase(Locale.ROOT)) {
            case "start" -> Request.of(Action.JFR_START);
            case "stop" -> Request.of(Action.JFR_STOP);
            case "dump" -> Request.of(Action.JFR_DUMP);
            default -> Request.error("'" + args[2] + "' must be one of: start, stop, dump");
        };
    }

    private static Request exactly(final String[] args, final int expected, final Action action) {
        return exactly(args, expected, action, USAGE);
    }

    private static Request exactly(
        final String[] args,
        final int expected,
        final Action action,
        final String usage
    ) {
        if (args.length != expected) {
            return Request.error("Usage: " + usage);
        }
        return Request.of(action);
    }

    private static List<String> filter(final List<String> candidates, final String prefix) {
        final String needle = prefix.toLowerCase(Locale.ROOT);
        final List<String> matches = new ArrayList<>();
        for (final String candidate : candidates) {
            if (candidate.startsWith(needle)) {
                matches.add(candidate);
            }
        }
        return List.copyOf(matches);
    }

    /**
     * Subcommands of {@code /tuff}.
     */
    public enum Action {

        /**
         * Engine summary.
         */
        SUMMARY,

        /**
         * Per consumer breakdown.
         */
        CONSUMERS,

        /**
         * Running tasks and the slowest recent completions.
         */
        TASKS,

        /**
         * Re-read {@code config/tuff.yml} and apply what the running engine can adopt.
         */
        RELOAD,

        /**
         * Pools, coverage, degradation and unknown threads of the unified thread layer.
         */
        THREADS,

        /**
         * Active region scheduler and, under Kairos, its fund, wait queue and supervisor.
         */
        SCHEDULER,

        /**
         * Worst recent permit waits of the Kairos fund.
         */
        SCHEDULER_LEASES,

        /**
         * Budget ledger, usable cores and degraded reasons of the thread layer and the fund.
         */
        SCHEDULER_BUDGET,

        /**
         * Zero the scheduler's A/B counters without touching the fund.
         */
        SCHEDULER_RESET,

        /**
         * Usage of the scheduler subcommand.
         */
        SCHEDULER_HELP,

        /**
         * Start a Flight Recorder recording that includes the compute events.
         */
        JFR_START,

        /**
         * Stop the recording and write it to {@code debug/}.
         */
        JFR_STOP,

        /**
         * Copy the current recording to {@code debug/} without stopping it.
         */
        JFR_DUMP
    }

    /**
     * Either an action or a message for the sender.
     *
     * @param action the action to run, {@code null} when parsing failed
     * @param error  the problem to show, {@code null} when parsing succeeded
     * @param limit  requested count for {@link Action#TASKS} and {@link Action#SCHEDULER_LEASES},
     *               {@code 0} otherwise
     */
    public record Request(@Nullable Action action, @Nullable String error, int limit) {

        /**
         * Validates the request.
         *
         * @param action the action to run, {@code null} when parsing failed
         * @param error  the problem to show, {@code null} when parsing succeeded
         * @param limit  requested number of tasks for {@link Action#TASKS}
         * @throws IllegalArgumentException if exactly one of the action and the error is {@code null}
         */
        public Request {
            if ((action == null) == (error == null)) {
                throw new IllegalArgumentException("a request carries either an action or an error");
            }
        }

        /**
         * Creates a successful request.
         *
         * @param action the action to run
         * @return the request
         */
        static Request of(final Action action) {
            return new Request(action, null, 0);
        }

        /**
         * Creates a task listing request.
         *
         * @param limit number of tasks to show, already clamped into the accepted range
         * @return the request
         */
        static Request tasks(final int limit) {
            return new Request(Action.TASKS, null, limit);
        }

        /**
         * Creates a permit wait listing request.
         *
         * @param limit number of waits to show, already clamped into the accepted range
         * @return the request
         */
        static Request leases(final int limit) {
            return new Request(Action.SCHEDULER_LEASES, null, limit);
        }

        /**
         * Creates a failed request.
         *
         * @param message what to tell the sender
         * @return the request
         */
        static Request error(final String message) {
            return new Request(null, message, 0);
        }

        /**
         * Checks whether parsing failed.
         *
         * @return {@code true} if {@link #error()} is set
         */
        public boolean failed() {
            return this.error != null;
        }
    }
}
