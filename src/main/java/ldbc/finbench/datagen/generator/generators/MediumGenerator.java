package ldbc.finbench.datagen.generator.generators;

import java.util.Iterator;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.dictionary.MediumNameDictionary;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class MediumGenerator {

    private DegreeDistribution degreeDistribution;
    private MediumNameDictionary mediumNameDictionary;
    private RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public MediumGenerator(GeneratorConfiguration conf) {
        this.randomFarm = new RandomGeneratorFarm();
        this.degreeDistribution = DatagenParams.getDegreeDistribution();
        this.degreeDistribution.initialize(conf);
        this.mediumNameDictionary = new MediumNameDictionary();
    }

    private long composeMediumId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 42);
        long bucket =
                (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                        .getSimulationEnd());
        return (bucket << 42) | ((id & idMask));
    }

    public Medium generateMedium() {

        long creationDate = Dictionaries.dates.randomMediumCreationDate(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        long mediumId = composeMediumId(nextId++, creationDate);
        String mediunName = Dictionaries.mediumNames.getGeoDistRandomName(
                randomFarm.get(RandomGeneratorFarm.Aspect.MEDIUM_NAME),
                mediumNameDictionary.getNumNames());
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        boolean isBlocked = false;

        return new Medium(mediumId, mediunName, creationDate, maxDegree, isBlocked);

    }

    public Iterator<Medium> generateMediumBlock(int blockId, int blockSize) {
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
