package ldbc.finbench.datagen.generation.generators;

import static net.andreinc.mockneat.types.enums.DomainSuffixType.POPULAR;
import static net.andreinc.mockneat.types.enums.HostNameType.ADVERB_VERB;
import static net.andreinc.mockneat.types.enums.MarkovChainType.KAFKA;
import static net.andreinc.mockneat.types.enums.URLSchemeType.HTTP;
import static net.andreinc.mockneat.types.enums.URLSchemeType.HTTPS;
import static net.andreinc.mockneat.unit.networking.URLs.urls;
import static net.andreinc.mockneat.unit.text.Markovs.markovs;

import java.util.Iterator;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;
import net.andreinc.mockneat.unit.networking.URLs;
import net.andreinc.mockneat.unit.text.Markovs;

public class CompanyGenerator {
    private final RandomGeneratorFarm randomFarm;
    private final Markovs descriptionGenerator;
    private final URLs urlGenerator;
    private int nextId = 0;

    public CompanyGenerator() {
        this.randomFarm = new RandomGeneratorFarm();
        this.descriptionGenerator = markovs();
        this.urlGenerator = urls();
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
        // Set creation date
        long creationDate = Dictionaries.dates.randomCompanyCreationDate(
            randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_DATE));
        company.setCreationDate(creationDate);
        // Set company id
        long companyId = composeCompanyId(nextId++, creationDate);
        company.setCompanyId(companyId);
        // Set company name
        String companyName =
            Dictionaries.companyNames.getUniformDistRandName(randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_NAME));
        company.setCompanyName(companyName);

        // Set country and city
        int countryId = Dictionaries.places.getCountryForPerson(randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_COUNTRY));
        company.setCountryId(countryId);
        company.setCityId(Dictionaries.places.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_CITY), countryId));

        // Set business
        company.setBusiness(Dictionaries.businessTypes.getUniformDistRandomType(randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY_BUSINESS)));

        // Set description TODO: use a better description
        String descrption = descriptionGenerator.size(DatagenParams.companyDescriptionMaxLength).type(KAFKA).get();
        company.setDescription(descrption);

        // Set url
        String url = urlGenerator.scheme(HTTPS).domain(POPULAR).host(ADVERB_VERB).get();
        company.setUrl(url);

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
