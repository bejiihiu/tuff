package kz.bejiihiu.tuff.threads.scenario;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Entry point for the cross-feature scenario tests. The server test task only runs classes that end
 * with {@code TestSuite}, so this suite is what pulls the plain JUnit scenarios in.
 */
@Suite(failIfNoTests = false)
@SuiteDisplayName("Tuff thread scenario tests")
@SelectPackages("kz.bejiihiu.tuff.threads.scenario")
public class TuffScenarioTestSuite {
}
