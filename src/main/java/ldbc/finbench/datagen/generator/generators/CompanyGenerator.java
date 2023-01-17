package ldbc.finbench.datagen.generator.generators;

import java.util.Iterator;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.CompanyNameDictionary;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyGenerator {

    private DegreeDistribution degreeDistribution;
    private CompanyNameDictionary companyNameDictionary;
    private RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public CompanyGenerator(GeneratorConfiguration conf) {
        this.randomFarm = new RandomGeneratorFarm();
        this.degreeDistribution = DatagenParams.getDegreeDistribution();
        this.degreeDistribution.initialize(conf);
        this.companyNameDictionary = new CompanyNameDictionary();
    }

    private long composeCompanyId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 40);
        long bucket =
                (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 40) | ((id & idMask));
    }

    public Company generateCompany() {

        long creationDate = Dictionaries.dates.randomCompanyCreationDate(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        long companyId = composeCompanyId(nextId++, creationDate);
        String companyName = Dictionaries.companyNames.getGeoDistRandomName(
                randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_NAME),
                companyNameDictionary.getNumNames());
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        boolean isBlocked = false;

        return new Company(companyId, companyName, creationDate, maxDegree, isBlocked);

    }

    public Iterator<Company> generateCompanyBlock(int blockId, int blockSize) {
        nextId = blockId * blockSize;
        return new Iterator<Company>() {
            private int companyNum = 0;

            @Override
            public boolean hasNext() {
                return companyNum < blockSize;
            }

            @Override
            public Company next() {
                ++companyNum;
                return generateCompany();
            }
        };
    }

}
