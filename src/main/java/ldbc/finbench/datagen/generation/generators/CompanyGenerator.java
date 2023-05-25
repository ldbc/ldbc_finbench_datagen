package ldbc.finbench.datagen.generation.generators;

import java.util.Iterator;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyGenerator {
    private final RandomGeneratorFarm randomFarm;
    private final Random random; // first random long for person, second for company
    private int nextId = 0;

    public CompanyGenerator() {
        this.randomFarm = new RandomGeneratorFarm();
        this.random = new Random(DatagenParams.defaultSeed);
    }

    private long composeCompanyId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 40);
        long bucket =
            (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 40) | ((id & idMask));
    }

    public Company generateCompany() {
        Company company = new Company();

        long creationDate = Dictionaries.dates.randomCompanyCreationDate(
            randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_DATE));
        company.setCreationDate(creationDate);

        long companyId = composeCompanyId(nextId++, creationDate);
        company.setCompanyId(companyId);

        String companyName =
            Dictionaries.companyNames.getUniformDistRandName(randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_NAME));
        company.setCompanyName(companyName);

        // Set blocked to false by default
        company.setBlocked(false);

        return company;
    }

    private void resetState(int seed) {
        random.setSeed(7654321L + 1234567L * seed);
        random.nextLong();
        long newSeed = random.nextLong();
        randomFarm.resetRandomGenerators(newSeed);
    }

    public Iterator<Company> generateCompanyBlock(int blockId, int blockSize) {
        resetState(blockId);
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
