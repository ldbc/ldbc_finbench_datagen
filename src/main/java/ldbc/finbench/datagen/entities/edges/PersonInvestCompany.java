package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class PersonInvestCompany implements DynamicActivity, Serializable {
    private final Person person;
    private final Company company;
    private double ratio;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;

    public PersonInvestCompany(Person person, Company company,
                               long creationDate, long deletionDate, double ratio, boolean isExplicitlyDeleted) {
        this.person = person;
        this.company = company;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.ratio = ratio;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonInvestCompany createPersonInvestCompany(Random dateRandom, Random ratioRandom, Person person,
                                                                Company company) {
        long creationDate = Dictionaries.dates.randomPersonToCompanyDate(dateRandom, person, company);
        double ratio = ratioRandom.nextDouble();
        PersonInvestCompany personInvestCompany = new PersonInvestCompany(person, company, creationDate, 0, ratio,
                                                                          false);
        person.getPersonInvestCompanies().add(personInvestCompany);

        return personInvestCompany;
    }

    public void scaleRatio(double sum) {
        this.ratio = this.ratio / sum;
    }

    public double getRatio() {
        return ratio;
    }

    public Company getCompany() {
        return company;
    }

    public Person getPerson() {
        return person;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }
}
