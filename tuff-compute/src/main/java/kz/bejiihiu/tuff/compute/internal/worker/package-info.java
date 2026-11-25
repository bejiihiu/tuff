/**
 * Worker threads and the loop they execute.
 *
 * <p>The pool owns thread lifecycle only: how many threads exist, when they are created, when they
 * retire, and whether the pool is shutting down. It does not know what a task is. Each worker thread
 * repeatedly calls {@link kz.bejiihiu.tuff.compute.internal.worker.WorkerLoop#executeNext(int)}, which
 * the engine implements as "claim, run, complete, record".
 *
 * <p>The split exists so the trickiest part of the scheduler — executing arbitrary user code and
 * surviving its failures — is not entangled with thread park/unpark logic and work stealing.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.worker;

import org.jspecify.annotations.NullMarked;
