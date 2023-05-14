package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class PersonInvestCompany implements DynamicActivity, Serializable {
    private Person person;
    private Company company;
    private double ratio;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

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

    public void setRatio(double ratio) {
        this.ratio = ratio;
    }

    public Company getCompany() {
        return company;
    }

    public void setCompany(Company company) {
        this.company = company;
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }
}
