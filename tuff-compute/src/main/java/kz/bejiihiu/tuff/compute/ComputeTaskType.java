package kz.bejiihiu.tuff.compute;

/**
 * Coarse category of a compute task.
 *
 * <p>Task types are accounting buckets only: the scheduler never inspects a task body and never lets
 * the type change how work is executed. A consumer can restrict which types it is allowed to submit
 * through {@link ComputeConsumer#allowedTaskTypes()}, quotas and metrics are reported per type, and
 * that is the whole job of this enum.
 *
 * <p>A subsystem that does not fit an existing category should use {@link #GENERAL} rather than
 * inventing a parallel scheduler.
 */
public enum ComputeTaskType {

    /**
     * Work that does not belong to a specialised category.
     */
    GENERAL,

    /**
     * Terrain, chunk or structure generation.
     */
    CHUNK_GENERATION,

    /**
     * Path and route search, for entities, mobs or anything else that walks.
     */
    PATHFINDING,

    /**
     * Artificial intelligence and behaviour tree evaluation.
     */
    AI_COMPUTATION,

    /**
     * Work a downstream consumer categorises on its own; still treated as ordinary compute work.
     */
    CUSTOM
}
