package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComputeCommandTest {

    @Test
    void emptyArgumentsAndComputeShowTheSummary() {
        assertAction(ComputeCommand.Action.SUMMARY);
        assertAction(ComputeCommand.Action.SUMMARY, "compute");
        assertAction(ComputeCommand.Action.SUMMARY, "COMPUTE");
        assertAction(ComputeCommand.Action.SUMMARY, "status");
    }

    @Test
    void topLevelShorthandsMapToTheirComputeActions() {
        assertAction(ComputeCommand.Action.RELOAD, "reload");
        assertAction(ComputeCommand.Action.RELOAD, "compute", "reload");
    }

    @Test
    void consumersSubcommandIsRecognized() {
        assertAction(ComputeCommand.Action.CONSUMERS, "compute", "consumers");
        assertAction(ComputeCommand.Action.CONSUMERS, "compute", "CONSUMERS");
    }

    @Test
    void tasksSubcommandDefaultsToTenAndClampsTheCount() {
        assertTasks(ComputeCommand.DEFAULT_TASK_LIMIT, "compute", "tasks");
        assertTasks(7, "compute", "tasks", "7");
        assertTasks(ComputeCommand.MAX_TASK_LIMIT, "compute", "tasks", "500");
        assertTasks(1, "compute", "tasks", "0");
        assertTasks(1, "compute", "tasks", "-4");
    }

    @Test
    void aBrokenTaskCountExplainsTheAcceptedRange() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"compute", "tasks", "many"});

        assertTrue(request.failed());
        assertTrue(request.error().contains("between 1 and " + ComputeCommand.MAX_TASK_LIMIT), request.error());
    }

    @Test
    void extraTaskArgumentsAreRejected() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"compute", "tasks", "5", "more"});

        assertTrue(request.failed());
        assertTrue(request.error().contains("/tuff compute tasks [n]"), request.error());
    }

    @Test
    void jfrSubcommandsAreRecognized() {
        assertAction(ComputeCommand.Action.JFR_START, "compute", "jfr", "start");
        assertAction(ComputeCommand.Action.JFR_STOP, "compute", "jfr", "STOP");
        assertAction(ComputeCommand.Action.JFR_DUMP, "compute", "jfr", "dump");
    }

    @Test
    void jfrWithoutAnOperationExplainsTheUsage() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"compute", "jfr"});

        assertTrue(request.failed());
        assertTrue(request.error().contains("start|stop|dump"), request.error());
    }

    @Test
    void unknownSubcommandsAreRejected() {
        assertError("'nonsense' must be one of: consumers, tasks, reload, jfr", "compute", "nonsense");
        assertError("'dumpme' must be one of: start, stop, dump", "compute", "jfr", "dumpme");
        assertError("Unknown subcommand 'oh no'", "oh no");
    }

    @Test
    void extraArgumentsAreRejectedWithTheUsageLine() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"compute", "consumers", "more"});

        assertTrue(request.failed());
        assertTrue(request.error().contains(ComputeCommand.USAGE), request.error());
    }

    @Test
    void failedRequestsCarryNoAction() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"compute", "reload", "now"});

        assertTrue(request.failed());
        assertNull(request.action());
    }

    @Test
    void threadsSubcommandShowsThePoolSummary() {
        assertAction(ComputeCommand.Action.THREADS, "threads");
        assertAction(ComputeCommand.Action.THREADS, "THREADS");
    }

    @Test
    void schedulerSubcommandShowsTheFund() {
        assertAction(ComputeCommand.Action.SCHEDULER, "scheduler");
        assertAction(ComputeCommand.Action.SCHEDULER, "SCHEDULER");
    }

    @Test
    void schedulerDetailsAreRecognized() {
        assertAction(ComputeCommand.Action.SCHEDULER_LEASES, "scheduler", "leases");
        assertAction(ComputeCommand.Action.SCHEDULER_BUDGET, "scheduler", "budget");
        assertAction(ComputeCommand.Action.SCHEDULER_RESET, "scheduler", "RESET");
        assertAction(ComputeCommand.Action.SCHEDULER_HELP, "scheduler", "help");
        assertAction(ComputeCommand.Action.SCHEDULER_HELP, "scheduler", "HELP");
    }

    @Test
    void leaseListingDefaultsToFiveAndClampsTheCount() {
        assertLeases(ComputeCommand.DEFAULT_LEASE_LIMIT, "scheduler", "leases");
        assertLeases(7, "scheduler", "leases", "7");
        assertLeases(ComputeCommand.MAX_LEASE_LIMIT, "scheduler", "leases", "500");
        assertLeases(1, "scheduler", "leases", "0");
        assertLeases(1, "scheduler", "leases", "-4");
    }

    @Test
    void aBrokenLeaseCountExplainsTheAcceptedRange() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"scheduler", "leases", "many"});

        assertTrue(request.failed());
        assertTrue(request.error().contains("between 1 and " + ComputeCommand.MAX_LEASE_LIMIT), request.error());
    }

    @Test
    void extraLeaseArgumentsAreRejected() {
        final ComputeCommand.Request request = ComputeCommand.parse(
            new String[] {"scheduler", "leases", "5", "more"}
        );

        assertTrue(request.failed());
        assertTrue(request.error().contains("/tuff scheduler leases [n]"), request.error());
    }

    @Test
    void anUnknownSchedulerSubcommandShowsTheSchedulerUsage() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"scheduler", "nonsense"});

        assertTrue(request.failed());
        assertTrue(request.error().contains(ComputeCommand.SCHEDULER_USAGE), request.error());
    }

    @Test
    void schedulerDetailsRejectExtraArguments() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"scheduler", "budget", "now"});

        assertTrue(request.failed());
        assertTrue(request.error().contains(ComputeCommand.SCHEDULER_USAGE), request.error());
    }

    @Test
    void schedulerSubcommandCompletesItsOwnGrammar() {
        assertEquals(List.of("leases", "budget", "reset", "help"), completions("scheduler", ""));
        assertEquals(List.of("leases"), completions("scheduler", "l"));
        assertEquals(List.of(), completions("scheduler", "leases", ""));
        assertEquals(List.of(), completions("scheduler", "budget", ""));
        assertEquals(List.of(), completions("scheduler", "reset", ""));
    }

    @Test
    void completionsWalkDownTheArgumentTree() {
        assertEquals(List.of("compute", "status", "reload", "threads", "scheduler"), completions(""));
        assertEquals(List.of("compute"), completions("c"));
        assertEquals(List.of("consumers", "tasks", "reload", "jfr"), completions("compute", ""));
        assertEquals(List.of("tasks"), completions("compute", "t"));
        assertEquals(List.of("jfr"), completions("compute", "j"));
        assertEquals(List.of("start", "stop"), completions("compute", "jfr", "s"));
        assertEquals(List.of("scheduler"), completions("sch"));
        assertEquals(List.of(), completions("compute", "tasks", ""));
        assertEquals(List.of(), completions("compute", "consumers", ""));
        assertEquals(List.of(), completions("reload", ""));
        assertEquals(List.of(), completions());
    }

    @Test
    void completionsAreCaseInsensitiveOnThePrefix() {
        assertEquals(List.of("consumers"), completions("COMPUTE", "C"));
    }

    @Test
    void errorMessagesArePlayerReadable() {
        final ComputeCommand.Request request = ComputeCommand.parse(new String[] {"compute", "jfr", "record"});

        assertTrue(request.failed());
        assertFalse(request.error().isBlank());
        assertFalse(request.error().contains("\n"));
    }

    private static void assertAction(final ComputeCommand.Action expected, final String... args) {
        final ComputeCommand.Request request = ComputeCommand.parse(args);
        assertFalse(request.failed(), () -> "unexpected error: " + request.error());
        assertEquals(expected, request.action());
    }

    private static void assertTasks(final int expectedLimit, final String... args) {
        final ComputeCommand.Request request = ComputeCommand.parse(args);
        assertFalse(request.failed(), () -> "unexpected error: " + request.error());
        assertEquals(ComputeCommand.Action.TASKS, request.action());
        assertEquals(expectedLimit, request.limit());
    }

    private static void assertLeases(final int expectedLimit, final String... args) {
        final ComputeCommand.Request request = ComputeCommand.parse(args);
        assertFalse(request.failed(), () -> "unexpected error: " + request.error());
        assertEquals(ComputeCommand.Action.SCHEDULER_LEASES, request.action());
        assertEquals(expectedLimit, request.limit());
    }

    private static void assertError(final String expectedPart, final String... args) {
        final ComputeCommand.Request request = ComputeCommand.parse(args);
        assertTrue(request.failed(), "expected a parse error");
        assertTrue(request.error().contains(expectedPart), request.error());
    }

    private static List<String> completions(final String... args) {
        return ComputeCommand.completions(args);
    }
}
