package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class PersonInvestCompany implements DynamicActivity, Serializable {

    private Person person;
    private Company company;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonInvestCompany(Person person, Company company,
                               long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.person = person;
        this.company = company;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonInvestCompany createPersonInvestCompany(Random random, Person person, Company company) {
        long creationDate = Dictionaries.dates.randomPersonToCompanyDate(random, person, company);
        PersonInvestCompany personInvestCompany = new PersonInvestCompany(person, company, creationDate, 0, false);
        person.getPersonInvestCompanies().add(personInvestCompany);

        return personInvestCompany;
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
