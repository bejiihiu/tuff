package kz.bejiihiu.tuff.threads.scenario;

import java.util.List;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.bootstrap.ComputeUtmiAdapter;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.pressure.TickPressureController;
import kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor;
import kz.bejiihiu.tuff.threads.pressure.TickPressureSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Scenario 1: a lagging region walks the whole chain - sampler pass, pressure controller, adapter sink,
 * engine budget - and the applied budget drops by exactly the reserve and comes back when the regions
 * are calm.
 *
 * <p>The arithmetic is deterministic: sixteen processors, one reserved core, one network core held
 * back, ten usable cores, so the compute pool starts at ten and yields to nine and eight.
 */
@Timeout(120)
class PressureBudgetScenarioTest {

    @Test
    void aLaggingRegionTakesExactlyTheReserveAndGivesItBackWhenCalm() {
        final ThreadManagementSettings settings = ScenarioFixtures.settings(
            true, 1, false, ScenarioFixtures.pressure(true), true
        );
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16, settings, ScenarioFixtures.network(1, 1)
        );
        final ComputeSettings compute = ComputeSettings.defaults();
        final TickPressureMonitor monitor = new TickPressureMonitor(
            ScenarioFixtures.pressure(true), harness.controller::usableCores
        );
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(harness.runtime, compute, monitor);
        harness.refresh();
        monitor.bind(adapter.pressureSink());
        final ScenarioFixtures.RecordingEngine engine = new ScenarioFixtures.RecordingEngine(compute);
        adapter.bind(engine);

        assertEquals(10, harness.controller.usableCores(), "16 processors: 12 ceiling, 1 reserve, 1 network");
        assertEquals(9, adapter.processors(),
            "the virtual lane's pump shares the remainder, so the platform pool starts one core short");
        assertEquals(2, monitor.maxReserveCores(), "the policy allows two cores, the floor would allow more");

        // five lagging seconds: enter, then the cooldown blocks the second step until second five
        for (int second = 0; second <= 5; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.lagging()), harness.clock.nanoTime());
            harness.clock.advanceSeconds(1);
        }
        assertEquals(List.of(8, 7), engine.budgets(), "one applied budget per reserve change, each exactly reserve less");
        assertEquals(TickPressureController.Reason.LAG_STEP_UP, monitor.snapshot().reason());
        assertEquals(TickPressureController.State.PRESSURE, monitor.snapshot().state());

        // six calm seconds: three windows plus the cooldown release the first core
        for (int second = 0; second <= 5; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.calm()), harness.clock.nanoTime());
            harness.clock.advanceSeconds(1);
        }
        assertEquals(List.of(8, 7, 8), engine.budgets(), "the warm-up windows must not change anything");
        final TickPressureSnapshot halfway = monitor.snapshot();
        assertNotNull(halfway);
        assertEquals(1, halfway.reserve());
        assertEquals(8, adapter.processors(), "the applied budget is the pool budget minus the reserve");
        assertEquals(10, halfway.usableCores());
        assertEquals(2, halfway.maxReserve());
        assertEquals(TickPressureController.Reason.CALM_STEP_DOWN, halfway.reason());

        // another six calm seconds: the last core is given back and the pool is whole again
        for (int second = 0; second <= 5; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.calm()), harness.clock.nanoTime());
            harness.clock.advanceSeconds(1);
        }
        assertEquals(List.of(8, 7, 8, 9), engine.budgets(), "four changes, four applications, no duplicates");
        assertEquals(0, monitor.reserve());
        assertEquals(9, adapter.processors(), "the pool has its whole budget back");
        final TickPressureSnapshot done = monitor.snapshot();
        assertNotNull(done);
        assertEquals(TickPressureController.State.NORMAL, done.state());
        assertEquals(TickPressureController.Reason.CALM_EXIT, done.reason());
        assertEquals(10, done.usableCores());
    }
}
