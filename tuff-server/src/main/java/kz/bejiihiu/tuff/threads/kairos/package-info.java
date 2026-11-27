/**
 * The first Kairos prototype: a deadline scheduler with a fixed fund of tick threads.
 *
 * <p>The package is deliberately small. It ports the EDF queue, comparator and runner state machine
 * from leafpile one to one and adds nothing else: no elasticity, no budget port, no affinity, no
 * catch up. Those are later packages with their own gates. The only extra piece is the experimental
 * opt in, because a scheduler an operator did not ask for must never start.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.kairos;

import org.jspecify.annotations.NullMarked;
