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
 *     <li>{@code /tuff compute reload} - re-read {@code config/tuff.yml};</li>
 *     <li>{@code /tuff compute jfr start|stop|dump} - Flight Recorder control;</li>
 *     <li>{@code /tuff status} and {@code /tuff reload} - shorthands for the two commands above.</li>
 * </ul>
 */
public final class ComputeCommand {

    /**
     * Usage line shown on argument errors and in {@code /help}.
     */
    public static final String USAGE = "/tuff [compute] [consumers|reload|jfr <start|stop|dump>]";

    private static final List<String> ROOT = List.of("compute", "status", "reload");
    private static final List<String> COMPUTE = List.of("consumers", "reload", "jfr");
    private static final List<String> JFR = List.of("start", "stop", "dump");

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
            case "compute" -> parseCompute(args);
            default -> Request.error("Unknown subcommand '" + args[0] + "'. Usage: " + USAGE);
        };
    }

    private static Request parseCompute(final String[] args) {
        if (args.length == 1) {
            return Request.of(Action.SUMMARY);
        }
        return switch (args[1].toLowerCase(Locale.ROOT)) {
            case "consumers" -> exactly(args, 2, Action.CONSUMERS);
            case "reload" -> exactly(args, 2, Action.RELOAD);
            case "jfr" -> parseJfr(args);
            default -> Request.error("'" + args[1] + "' must be one of: consumers, reload, jfr");
        };
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
        if (index >= 1 && !args[0].equalsIgnoreCase("compute")) {
            return List.of();
        }
        if (index == 2 && !args[1].equalsIgnoreCase("jfr")) {
            return List.of();
        }
        final List<String> candidates = switch (index) {
            case 0 -> ROOT;
            case 1 -> COMPUTE;
            case 2 -> JFR;
            default -> List.of();
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
        if (args.length != expected) {
            return Request.error("Usage: " + USAGE);
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
         * Re-read {@code config/tuff.yml} and apply what the running engine can adopt.
         */
        RELOAD,

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
     */
    public record Request(@Nullable Action action, @Nullable String error) {

        /**
         * Validates the request.
         *
         * @param action the action to run, {@code null} when parsing failed
         * @param error  the problem to show, {@code null} when parsing succeeded
         * @throws IllegalArgumentException if exactly one of the components is {@code null}
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
            return new Request(action, null);
        }

        /**
         * Creates a failed request.
         *
         * @param message what to tell the sender
         * @return the request
         */
        static Request error(final String message) {
            return new Request(null, message);
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
