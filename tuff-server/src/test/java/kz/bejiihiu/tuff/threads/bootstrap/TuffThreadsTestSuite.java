package kz.bejiihiu.tuff.threads.bootstrap;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Entry point for the thread layer tests. The server test task only runs classes that end with
 * {@code TestSuite}, so this suite is what pulls the plain JUnit tests of the bootstrap and probe
 * packages in.
 */
@Suite(failIfNoTests = false)
@SuiteDisplayName("Tuff thread layer tests")
@SelectPackages({
    "kz.bejiihiu.tuff.threads.bootstrap",
    "kz.bejiihiu.tuff.threads.bridge",
    "kz.bejiihiu.tuff.threads.observe",
    "kz.bejiihiu.tuff.threads.probe",
    "kz.bejiihiu.tuff.threads.region"
})
public class TuffThreadsTestSuite {
}
