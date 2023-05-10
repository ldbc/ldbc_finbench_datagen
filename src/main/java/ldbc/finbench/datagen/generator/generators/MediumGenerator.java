package ldbc.finbench.datagen.generator.generators;

import java.util.Iterator;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class MediumGenerator {
    private final RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public MediumGenerator(GeneratorConfiguration conf) {
        this.randomFarm = new RandomGeneratorFarm();
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

        // Set blocked to false by default
        medium.setBlocked(false);

        return medium;
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
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
