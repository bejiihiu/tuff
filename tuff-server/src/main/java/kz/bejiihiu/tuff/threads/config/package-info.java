/**
 * Configuration of the thread layer.
 *
 * <p>This package reads the {@code thread-management} section of {@code config/tuff.yml} and turns it
 * into a {@link kz.bejiihiu.tuff.threads.ThreadManagementSettings} tree. The section shares the file,
 * the default template and the "a bad value falls back to its default" rule with the compute
 * configuration, but it is parsed separately: the two subsystems start and stop in different places and
 * must not depend on each other's snapshot.
 *
 * <p>The whole section is read once, at startup. No key is applied to a running server, which is why
 * every changed key is reported as "takes effect after a restart" by
 * {@link kz.bejiihiu.tuff.compute.config.ConfigDiff}.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.config;

import org.jspecify.annotations.NullMarked;
