package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class PersonInvestCompany implements DynamicActivity, Serializable {
    private long personId;
    private long companyId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonInvestCompany(long personId, long companyId,
                               long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.personId = personId;
        this.companyId = companyId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createPersonInvestCompany(Random random, Person person, Company company) {
        long creationDate = Dictionaries.dates.randomPersonToCompanyDate(random, person, company);

        person.getPersonInvestCompanies().add(new PersonInvestCompany(person.getPersonId(),company.getCompanyId(),
                creationDate, 0, false));
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(long personId) {
        this.personId = personId;
    }

    public long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(long companyId) {
        this.companyId = companyId;
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
