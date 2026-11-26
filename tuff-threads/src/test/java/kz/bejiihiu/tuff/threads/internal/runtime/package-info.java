/**
 * Tests for the assembly and lifecycle of the unified thread layer.
 *
 * <p>The cases that matter here are order and honesty: the right components wired in the right order,
 * start and stop that can be called twice, an error instead of a silent grant after stop, and a reason
 * list that reaches the snapshot instead of staying inside the manager.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.runtime;

import org.jspecify.annotations.NullMarked;
