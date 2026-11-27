package kz.bejiihiu.tuff.bench;

import java.util.ArrayList;
import java.util.List;

/**
 * Geometry and size of the synthetic world, identical for every arm of a block.
 *
 * <p>Force-loaded areas are placed far apart so that each one becomes its own region: this is the only
 * way to get more than one ticking region without player clients, and the region count is the whole
 * point of comparing a fixed fund with an elastic one. The numbers are recorded in the manifest and
 * may not change between arms of the same block.
 *
 * @param areas           how many force-loaded areas, 1 to 4
 * @param areaChunks      side of one area in chunks
 * @param areaStep        distance between area origins in chunks
 * @param entitiesPerArea entities summoned per area by the profiles that summon
 * @param entityType      entity id without the {@code minecraft:} namespace
 */
record LoadSpec(int areas, int areaChunks, int areaStep, int entitiesPerArea, String entityType) {

    /**
     * Validates the shape.
     *
     * @throws IllegalArgumentException if a size is not positive
     */
    LoadSpec {
        if (areas < 1 || areas > 4) {
            throw new IllegalArgumentException("areas must be in [1, 4], got " + areas);
        }
        if (areaChunks < 1) {
            throw new IllegalArgumentException("areaChunks must be positive, got " + areaChunks);
        }
        if (areaStep < areaChunks) {
            throw new IllegalArgumentException(
                "areaStep " + areaStep + " must be at least areaChunks " + areaChunks
            );
        }
        if (entitiesPerArea < 0) {
            throw new IllegalArgumentException("entitiesPerArea must not be negative, got " + entitiesPerArea);
        }
        if (entityType.isBlank()) {
            throw new IllegalArgumentException("entityType must not be blank");
        }
    }

    /**
     * Returns the block bounds of one area, as {@code x0, z0, x1, z1}.
     *
     * @param index area index
     * @return inclusive block bounds
     */
    int[] bounds(final int index) {
        final int originX = areaOrigin(this.areaStep, index)[0];
        final int originZ = areaOrigin(this.areaStep, index)[1];
        final int x0 = originX * 16;
        final int z0 = originZ * 16;
        return new int[] {x0, z0, (originX + this.areaChunks) * 16 - 1, (originZ + this.areaChunks) * 16 - 1};
    }

    private static int[] areaOrigin(final int step, final int index) {
        final int row = index / 2;
        final int column = index % 2;
        return new int[] {column * step, row * step};
    }

    /**
     * Builds the command that force-loads one area.
     *
     * @param index area index
     * @return console command
     */
    String forceLoadCommand(final int index) {
        final int[] bounds = this.bounds(index);
        return "forceload add " + bounds[0] + ' ' + bounds[1] + ' ' + bounds[2] + ' ' + bounds[3];
    }

    /**
     * Builds the command that drops the tickets of one area.
     *
     * @param index area index
     * @return console command
     */
    String unforceLoadCommand(final int index) {
        final int[] bounds = this.bounds(index);
        return "forceload remove " + bounds[0] + ' ' + bounds[1] + ' ' + bounds[2] + ' ' + bounds[3];
    }

    /**
     * Builds summon commands laid out on a grid inside one area.
     *
     * @param index area index
     * @param count how many entities to summon
     * @return console commands
     */
    List<String> summonCommands(final int index, final int count) {
        if (count <= 0) {
            return List.of();
        }
        final int[] bounds = this.bounds(index);
        final int centreX = (bounds[0] + bounds[2]) / 2;
        final int centreZ = (bounds[1] + bounds[3]) / 2;
        final int side = Math.max(1, (int) Math.ceil(Math.sqrt(count)));
        final List<String> commands = new ArrayList<>(count);
        for (int entity = 0; entity < count; ++entity) {
            final int dx = entity % side - side / 2;
            final int dz = entity / side % side - side / 2;
            commands.add(
                "summon minecraft:" + this.entityType + ' '
                    + (centreX + dx * 3) + " -60 " + (centreZ + dz * 3)
                    + " {PersistenceRequired:1b,Silent:1b,NoAI:1b}"
            );
        }
        return List.copyOf(commands);
    }

    /**
     * Builds one summon command for a single entity in an area, used by the churn profile.
     *
     * @param index area index
     * @param count how many entities to summon
     * @return console commands
     */
    List<String> summonInAreaCommand(final int index, final int count) {
        return this.summonCommands(index, count);
    }
}
