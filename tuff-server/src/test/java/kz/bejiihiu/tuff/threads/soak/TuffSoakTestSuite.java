package kz.bejiihiu.tuff.threads.soak;

import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Entry point for the scheduler soak and fault-injection tests.
 *
 * <p>The server test task only runs classes whose name ends with {@code TestSuite}, so this suite is
 * what pulls the plain JUnit tests of this package in. The {@code soak} tag is applied to the suite so
 * a future selection can pick exactly these tests without touching the rest of the module.
 */
@Suite(failIfNoTests = false)
@SuiteDisplayName("Tuff scheduler soak and fault injection")
@SelectPackages("kz.bejiihiu.tuff.threads.soak")
@IncludeTags("soak")
public class TuffSoakTestSuite {
}
