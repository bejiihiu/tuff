package kz.bejiihiu.tuff.threads.relief;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Entry point for the relief trigger tests. The server test task only runs classes that end with
 * {@code TestSuite}, so this suite is what pulls the plain JUnit tests of the package in.
 */
@Suite(failIfNoTests = false)
@SuiteDisplayName("Tuff region relief trigger tests")
@SelectPackages("kz.bejiihiu.tuff.threads.relief")
public class TuffReliefTestSuite {
}
