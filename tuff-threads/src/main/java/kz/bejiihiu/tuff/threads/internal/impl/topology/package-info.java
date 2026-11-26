/**
 * Processor, container and NUMA detection.
 *
 * <p>Topology is read once and never acted upon: nothing here pins a thread, changes a scheduling
 * class or asks the OS to move work. The only decisions taken from this package are "how many cores may
 * we size against" and "how big is the machine next to the container".
 *
 * <p>Two counts exist on purpose. {@code availableProcessors} is what the process may actually use and
 * is the number every budget starts from; {@code possibleCpus} is the kernel's view of the machine and
 * only exists so an operator can see why a container gets fewer threads than the host has cores. Every
 * value that could not be read is {@code -1}, never zero: zero is a real answer ("nothing"), unknown is
 * not.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.topology;

import org.jspecify.annotations.NullMarked;
