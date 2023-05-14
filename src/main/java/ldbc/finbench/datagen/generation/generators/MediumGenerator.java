package ldbc.finbench.datagen.generation.generators;

import java.util.Iterator;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class MediumGenerator {
    private final RandomGeneratorFarm randomFarm;
    private final Random blockRandom;
    private int nextId = 0;

    public MediumGenerator() {
        this.randomFarm = new RandomGeneratorFarm();
        this.blockRandom = new Random(DatagenParams.defaultSeed);
    }

    private long composeMediumId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 42);
        long bucket =
            (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 42) | ((id & idMask));
    }

    public Medium generateMedium() {
        Medium medium = new Medium();

        long creationDate = Dictionaries.dates.randomMediumCreationDate(
            randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        medium.setCreationDate(creationDate);

        long mediumId = composeMediumId(nextId++, creationDate);
        medium.setMediumId(mediumId);

        String mediunName =
            Dictionaries.mediumNames.getUniformDistRandomName(randomFarm.get(RandomGeneratorFarm.Aspect.MEDIUM_NAME));
        medium.setMediumName(mediunName);

        // Set isBlocked
        medium.setBlocked(blockRandom.nextDouble() < DatagenParams.blockedMediumRatio);

        return medium;
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        blockRandom.setSeed(seed);
    }

    public Iterator<Medium> generateMediumBlock(int blockId, int blockSize) {
        resetState(blockId);
        nextId = blockId * blockSize;
        return new Iterator<Medium>() {
            private int mediumNum = 0;

            @Override
            public boolean hasNext() {
                return mediumNum < blockSize;
            }

            @Override
            public Medium next() {
                ++mediumNum;
                return generateMedium();
            }
        };
    }

}
