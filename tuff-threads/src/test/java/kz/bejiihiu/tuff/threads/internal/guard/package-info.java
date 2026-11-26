/**
 * Static guard against raw thread and pool creation in Tuff owned sources.
 *
 * <p>This is a test, not a checkstyle rule, because the allowlist needs reasons and because the check
 * must see comments stripped: a javadoc line that mentions {@code new Thread} is documentation, not a
 * hidden pool.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.guard;

import org.jspecify.annotations.NullMarked;
