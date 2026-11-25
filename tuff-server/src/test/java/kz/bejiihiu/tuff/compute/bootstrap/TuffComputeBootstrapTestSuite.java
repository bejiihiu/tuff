package kz.bejiihiu.tuff.compute.bootstrap;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Entry point for the compute bootstrap tests. The server test task only runs classes that end with
 * {@code TestSuite}, so this suite is what pulls the plain JUnit tests in the package in.
 */
@Suite(failIfNoTests = false)
@SuiteDisplayName("Tuff compute bootstrap tests")
@SelectPackages("kz.bejiihiu.tuff.compute.bootstrap")
public class TuffComputeBootstrapTestSuite {
}
