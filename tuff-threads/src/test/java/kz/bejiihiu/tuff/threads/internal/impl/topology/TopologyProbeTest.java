package kz.bejiihiu.tuff.threads.internal.impl.topology;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the probe over synthetic sysfs trees.
 */
class TopologyProbeTest {

    @Test
    void readsLinuxTopology(@TempDir final Path root) throws IOException {
        write(root, "devices/system/cpu/possible", "0-63\n");
        write(root, "devices/system/node/possible", "0-1\n");
        write(root, "fs/cgroup/cpu.max", "400000 100000\n");
        write(root, "fs/cgroup/cpuset.cpus.effective", "0-7\n");
        for (int cpu = 0; cpu < 8; cpu++) {
            write(root, "devices/system/cpu/cpu" + cpu + "/topology/thread_siblings_list", cpu % 4 + "," + (cpu % 4 + 4));
        }

        final TopologyProbe probe = TopologyProbe.read(root, 8);
        final TopologySnapshot snapshot = probe.snapshot();

        assertEquals(8, probe.availableProcessors());
        assertEquals(64, probe.totalCores());
        assertEquals(64, snapshot.possibleCpus());
        assertEquals(4, snapshot.physicalCores());
        assertEquals(8, snapshot.logicalCpus());
        assertEquals(2, snapshot.siblingsPerCore());
        assertEquals(4, snapshot.cgroupQuotaCores());
        assertEquals(8, snapshot.cgroupCpusetCpus());
        assertTrue(snapshot.numaAvailable());
        assertEquals(2, snapshot.numaNodeCount());
        assertTrue(snapshot.constrained());
        assertTrue(snapshot.quotaLimited());
        assertEquals("linux-sysfs", snapshot.source());
    }

    @Test
    void singleNumaNodeIsNotNuma(@TempDir final Path root) throws IOException {
        write(root, "devices/system/cpu/possible", "0-3\n");
        write(root, "devices/system/node/possible", "0\n");

        final TopologySnapshot snapshot = TopologyProbe.read(root, 4).snapshot();

        assertEquals(1, snapshot.numaNodeCount());
        assertFalse(snapshot.numaAvailable());
        assertFalse(TopologyProbe.read(root, 4).numaAvailable());
    }

    @Test
    void unknownFactsStayUnknown(@TempDir final Path root) {
        final TopologyProbe probe = TopologyProbe.read(root, 6);
        final TopologySnapshot snapshot = probe.snapshot();

        assertEquals(6, snapshot.availableProcessors());
        assertEquals(6, probe.totalCores(), "the diagnostics count falls back to what is known");
        assertEquals(-1, snapshot.possibleCpus());
        assertEquals(-1, snapshot.physicalCores());
        assertEquals(-1, snapshot.logicalCpus());
        assertEquals(-1, snapshot.siblingsPerCore());
        assertEquals(-1, snapshot.cgroupQuotaCores());
        assertEquals(-1, snapshot.cgroupCpusetCpus());
        assertEquals(1, snapshot.numaNodeCount());
        assertFalse(snapshot.numaAvailable());
        assertFalse(snapshot.constrained());
        assertEquals("runtime", snapshot.source());
    }

    @Test
    void readsCgroupV1QuotaAndCpuset(@TempDir final Path root) throws IOException {
        write(root, "devices/system/cpu/possible", "0-15\n");
        write(root, "fs/cgroup/cpu/cpu.cfs_quota_us", "150000\n");
        write(root, "fs/cgroup/cpu/cpu.cfs_period_us", "100000\n");
        write(root, "fs/cgroup/cpuset/cpuset.effective_cpus", "0-3\n");

        final TopologySnapshot snapshot = TopologyProbe.read(root, 2).snapshot();

        assertEquals(1, snapshot.cgroupQuotaCores());
        assertEquals(4, snapshot.cgroupCpusetCpus());
        assertTrue(snapshot.constrained());
    }

    @Test
    void unlimitedQuotaIsUnknown(@TempDir final Path root) throws IOException {
        write(root, "devices/system/cpu/possible", "0-3\n");
        write(root, "fs/cgroup/cpu.max", "max 100000\n");

        assertEquals(-1, TopologyProbe.read(root, 4).snapshot().cgroupQuotaCores());
    }

    @Test
    void siblingGroupsDecidePhysicalCores(@TempDir final Path root) throws IOException {
        write(root, "devices/system/cpu/possible", "0-1\n");
        write(root, "devices/system/cpu/cpu0/topology/thread_siblings_list", "0,1\n");
        write(root, "devices/system/cpu/cpu1/topology/thread_siblings_list", "0,1\n");

        final TopologySnapshot snapshot = TopologyProbe.read(root, 2).snapshot();

        assertEquals(1, snapshot.physicalCores());
        assertEquals(2, snapshot.logicalCpus());
        assertEquals(2, snapshot.siblingsPerCore());
    }

    @Test
    void parsesCpuLists() {
        assertEquals(8, TopologyProbe.parseCpuList("0-3,8-11"));
        assertEquals(1, TopologyProbe.parseCpuList("5"));
        assertEquals(4, TopologyProbe.parseCpuList("0,2,4,6"));
        assertEquals(1, TopologyProbe.parseCpuList(" 7 \n"));
        assertEquals(-1, TopologyProbe.parseCpuList(""));
        assertEquals(-1, TopologyProbe.parseCpuList("0-"));
        assertEquals(-1, TopologyProbe.parseCpuList("3-1"));
        assertEquals(-1, TopologyProbe.parseCpuList("0-1,oops"));
    }

    @Test
    void parsesQuotas() {
        assertEquals(4, TopologyProbe.parseCpuMax("400000 100000"));
        assertEquals(1, TopologyProbe.parseCpuMax("50000 100000"));
        assertEquals(-1, TopologyProbe.parseCpuMax("max 100000"));
        assertEquals(-1, TopologyProbe.parseCpuMax("garbage"));
        assertEquals(-1, TopologyProbe.parseCpuMax("0 100000"));
        assertEquals(2, TopologyProbe.parseQuota("200000", "100000"));
        assertEquals(-1, TopologyProbe.parseQuota("100000", "0"));
    }

    @Test
    void rejectsImpossibleReadings() {
        assertThrows(IllegalArgumentException.class, () -> new TopologySnapshot(
            0, -1, -1, -1, -1, -1, -1, 1, false, "runtime"
        ));
        assertThrows(IllegalArgumentException.class, () -> new TopologySnapshot(
            4, -2, -1, -1, -1, -1, -1, 1, false, "runtime"
        ));
        assertThrows(IllegalArgumentException.class, () -> new TopologySnapshot(
            4, -1, -1, -1, -1, -1, -1, 0, false, "runtime"
        ));
        assertThrows(IllegalArgumentException.class, () -> TopologyProbe.read(Path.of("."), 0));
    }

    @Test
    void runtimeOnlySnapshotIsHonest() {
        final TopologySnapshot snapshot = TopologySnapshot.runtimeOnly(3);

        assertEquals(3, snapshot.availableProcessors());
        assertEquals(-1, snapshot.possibleCpus());
        assertEquals(1, snapshot.numaNodeCount());
        assertFalse(snapshot.numaAvailable());
        assertEquals("runtime", snapshot.source());
    }

    private static void write(final Path root, final String relative, final String content) throws IOException {
        final Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.US_ASCII);
    }
}
