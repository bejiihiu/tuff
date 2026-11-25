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
        assertError("'nonsense' must be one of: consumers, reload, jfr", "compute", "nonsense");
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
    void completionsWalkDownTheArgumentTree() {
        assertEquals(List.of("compute", "status", "reload"), completions(""));
        assertEquals(List.of("compute"), completions("c"));
        assertEquals(List.of("consumers", "reload", "jfr"), completions("compute", ""));
        assertEquals(List.of("jfr"), completions("compute", "j"));
        assertEquals(List.of("start", "stop"), completions("compute", "jfr", "s"));
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

    private static void assertError(final String expectedPart, final String... args) {
        final ComputeCommand.Request request = ComputeCommand.parse(args);
        assertTrue(request.failed(), "expected a parse error");
        assertTrue(request.error().contains(expectedPart), request.error());
    }

    private static List<String> completions(final String... args) {
        return ComputeCommand.completions(args);
    }
}
