package ldbc.finbench.datagen.generator.generators;

import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.CompanyNameDictionary;
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
        this.degreeDistribution.initialize(conf);
        this.mediumNameDictionary = new MediumNameDictionary();
    }

    private long composeMediumId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 42);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 42) | ((id & idMask));
    }

    private Medium generateMedium() {

        long creationDate = Dictionaries.dates.randomMediumCreationDate(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        long mediumId = composeMediumId(nextId++, creationDate);
        String mediunName = Dictionaries.mediumNames.getGeoDistRandomName(
                randomFarm.get(RandomGeneratorFarm.Aspect.MEDIUM_NAME), mediumNameDictionary.getNumNames());
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        boolean isBlocked = false;

        return new Medium(mediumId,mediunName,creationDate,maxDegree,isBlocked);

    }

}
