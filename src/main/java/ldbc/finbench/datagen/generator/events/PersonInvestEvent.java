package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonInvestEvent implements Serializable {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public PersonInvestEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public List<PersonInvestCompany> personInvest(List<Person> persons, List<Company> companies, int blockId) {
        random.setSeed(blockId);
        List<PersonInvestCompany> personInvestCompanies = new ArrayList<>();

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            int companyIndex = randIndex.nextInt(companies.size());

            if (invest()) {
                PersonInvestCompany personInvestCompany = PersonInvestCompany.createPersonInvestCompany(
                    randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    p,
                    companies.get(companyIndex));
                personInvestCompanies.add(personInvestCompany);
            }
        }
        return personInvestCompanies;
    }

    private boolean invest() {
        //TODO determine whether to generate Invest
        return true;
    }
}
