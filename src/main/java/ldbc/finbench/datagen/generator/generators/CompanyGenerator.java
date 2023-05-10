package ldbc.finbench.datagen.generator.generators;

import java.util.Iterator;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyGenerator {
    private final RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public CompanyGenerator(GeneratorConfiguration conf) {
        this.randomFarm = new RandomGeneratorFarm();
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
            randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
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
        randomFarm.resetRandomGenerators(seed);
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
