package kz.bejiihiu.tuff.threads.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class ThreadsConfigTest {

    @TempDir
    Path directory;

    @Test
    void aMissingFileUsesTheDefaultsAndSaysWhy() {
        final ThreadsConfig config = new ThreadsConfig(this.directory.resolve("tuff.yml"));

        final ThreadsConfig.ThreadsLoadResult result = config.load();

        assertTrue(result.missingFile());
        assertEquals(ThreadManagementSettings.defaults(), result.settings());
        assertTrue(result.hasProblems());
        assertEquals(1, result.problems().size());
    }

    @Test
    void aValidSectionIsRead() throws IOException {
        final Path file = this.directory.resolve("tuff.yml");
        Files.writeString(file, """
            thread-management:
              enabled: false
              region-execution:
                threads: 4
            """, StandardCharsets.UTF_8);

        final ThreadsConfig.ThreadsLoadResult result = new ThreadsConfig(file).load();

        assertFalse(result.missingFile());
        assertTrue(result.problems().isEmpty(), () -> "unexpected problems: " + result.problems());
        assertFalse(result.settings().enabled());
        assertEquals(4, result.settings().regionExecution().threads());
    }

    @Test
    void anInvalidFileFallsBackToTheDefaultsInsteadOfThrowing() throws IOException {
        final Path file = this.directory.resolve("tuff.yml");
        Files.writeString(file, "thread-management: [unclosed", StandardCharsets.UTF_8);

        final ThreadsConfig.ThreadsLoadResult result = new ThreadsConfig(file).load();

        assertEquals(ThreadManagementSettings.defaults(), result.settings());
        assertTrue(result.hasProblems());
    }

    @Test
    void thePathIsAbsoluteForLogsAndCommands() {
        final ThreadsConfig config = new ThreadsConfig(Path.of("config", "tuff.yml"));

        assertTrue(config.file().isAbsolute());
        assertTrue(config.file().endsWith(Path.of("config", "tuff.yml")));
    }
}
