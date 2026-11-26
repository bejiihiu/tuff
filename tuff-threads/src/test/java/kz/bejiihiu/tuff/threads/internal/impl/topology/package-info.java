/**
 * Tests for the processor and container topology probe.
 *
 * <p>Every test builds a fake sysfs tree in a temporary directory and runs the real parser over it.
 * Nothing here reads the machine's own topology, so the assertions do not change when the developer's
 * laptop does.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.topology;

import org.jspecify.annotations.NullMarked;
