package kz.bejiihiu.tuff.threads.kairos;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Entry point for the Kairos scheduler tests. The server test task only runs classes that end with
 * {@code TestSuite}, so this suite is what pulls the plain JUnit tests of this package in.
 */
@Suite(failIfNoTests = false)
@SuiteDisplayName("Tuff Kairos scheduler tests")
@SelectPackages("kz.bejiihiu.tuff.threads.kairos")
public class TuffKairosTestSuite {
}
