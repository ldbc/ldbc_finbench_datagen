package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonInvestEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public PersonInvestEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<PersonInvestCompany> personInvest(List<Person> persons, List<Company> companies, int blockId) {
        resetState(blockId);
        List<PersonInvestCompany> personInvestCompanies = new ArrayList<>();

        // TODO: person can invest multiple companies
        for (Person p : persons) {
            int companyIndex = randIndex.nextInt(companies.size());
            PersonInvestCompany personInvestCompany = PersonInvestCompany.createPersonInvestCompany(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                p,
                companies.get(companyIndex));
            personInvestCompanies.add(personInvestCompany);
        }
        return personInvestCompanies;
    }
}
