/**
 * The per-region overload trigger of the region relief path.
 *
 * <p>The trigger sits on the scheduler independent tick feed and turns "this region keeps running
 * long" into a relief window. A window is a permission, not a task: registered work units are called
 * on the owning region thread when one opens, and each unit decides what to offload. Nothing in this
 * package touches world state.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.relief;

import org.jspecify.annotations.NullMarked;
