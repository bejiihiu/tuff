package kz.bejiihiu.tuff.compute.config;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Entry point for the compute configuration tests. The server test task only runs classes that end
 * with {@code TestSuite}, so this suite is what pulls the plain JUnit tests in the package in.
 */
@Suite(failIfNoTests = false)
@SuiteDisplayName("Tuff compute configuration tests")
@SelectPackages("kz.bejiihiu.tuff.compute.config")
public class TuffComputeConfigTestSuite {
}
