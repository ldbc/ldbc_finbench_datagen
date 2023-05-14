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
    private long creationDate;
    private boolean isBlocked;
    private WorkIn workIn;
    private List<PersonOwnAccount> personOwnAccounts;
    private List<PersonInvestCompany> personInvestCompanies;
    private List<PersonGuaranteePerson> guaranteeSrc;
    private List<PersonGuaranteePerson> guaranteeDst;
    private List<PersonApplyLoan> personApplyLoans;

    public Person() {
        personOwnAccounts = new ArrayList<>();
        personInvestCompanies = new ArrayList<>();
        guaranteeSrc = new ArrayList<>();
        guaranteeDst = new ArrayList<>();
        personApplyLoans = new ArrayList<>();
    }

    public boolean canGuarantee(Person to) {
        if (this.getPersonId() == to.getPersonId()) {
            return false;
        }
        // can not guarantee the same person twice
        for (PersonGuaranteePerson guarantee : guaranteeSrc) {
            if (guarantee.getToPerson().getPersonId() == to.getPersonId()) {
                return false;
            }
        }
        // can not guarantee cyclically
        for (PersonGuaranteePerson guarantee : guaranteeDst) {
            if (guarantee.getFromPerson().getPersonId() == to.getPersonId()) {
                return false;
            }
        }
        return true;
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

    public List<PersonGuaranteePerson> getGuaranteeSrc() {
        return guaranteeSrc;
    }

    public void setGuaranteeSrc(List<PersonGuaranteePerson> guaranteeSrc) {
        this.guaranteeSrc = guaranteeSrc;
    }

    public List<PersonGuaranteePerson> getGuaranteeDst() {
        return guaranteeDst;
    }

    public void setGuaranteeDst(List<PersonGuaranteePerson> guaranteeDst) {
        this.guaranteeDst = guaranteeDst;
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
