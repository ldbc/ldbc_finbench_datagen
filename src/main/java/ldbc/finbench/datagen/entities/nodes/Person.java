package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.edges.WorkIn;

public class Person implements Serializable {
    private long personId;
    private String personName;
    private byte gender;
    private WorkIn workIn;
    private long creationDate;
    //    private long maxDegree;
    private boolean isBlocked;
    private List<PersonOwnAccount> personOwnAccounts;
    private List<PersonInvestCompany> personInvestCompanies;
    private List<PersonGuaranteePerson> personGuaranteePeople;
    private List<PersonApplyLoan> personApplyLoans;

    public Person() {
        personOwnAccounts = new ArrayList<>();
        personInvestCompanies = new ArrayList<>();
        personGuaranteePeople = new ArrayList<>();
        personApplyLoans = new ArrayList<>();
    }

    public Person(long personId, String personName, byte gender, long creationDate, boolean isBlocked) {
        this.personId = personId;
        this.personName = personName;
        this.gender = gender;
        personOwnAccounts = new ArrayList<>();
        personInvestCompanies = new ArrayList<>();
        personGuaranteePeople = new ArrayList<>();
        personApplyLoans = new ArrayList<>();
        this.creationDate = creationDate;
        this.isBlocked = isBlocked;
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(long personId) {
        this.personId = personId;
    }

    public String getPersonName() {
        return personName;
    }

    public void setPersonName(String personName) {
        this.personName = personName;
    }

    public byte getGender() {
        return gender;
    }

    public void setGender(byte gender) {
        this.gender = gender;
    }

    public WorkIn getWorkIn() {
        return workIn;
    }

    public void setWorkIn(WorkIn workIn) {
        this.workIn = workIn;
    }

    public List<PersonOwnAccount> getPersonOwnAccounts() {
        return personOwnAccounts;
    }

    public void setPersonOwnAccounts(List<PersonOwnAccount> personOwnAccounts) {
        this.personOwnAccounts = personOwnAccounts;
    }

    public List<PersonInvestCompany> getPersonInvestCompanies() {
        return personInvestCompanies;
    }

    public void setPersonInvestCompanies(List<PersonInvestCompany> personInvestCompanies) {
        this.personInvestCompanies = personInvestCompanies;
    }

    public List<PersonGuaranteePerson> getPersonGuaranteePeople() {
        return personGuaranteePeople;
    }

    public void setPersonGuaranteePeople(List<PersonGuaranteePerson> personGuaranteePeople) {
        this.personGuaranteePeople = personGuaranteePeople;
    }

    public List<PersonApplyLoan> getPersonApplyLoans() {
        return personApplyLoans;
    }

    public void setPersonApplyLoans(List<PersonApplyLoan> personApplyLoans) {
        this.personApplyLoans = personApplyLoans;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }

}
