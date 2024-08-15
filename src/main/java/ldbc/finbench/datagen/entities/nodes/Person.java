package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Person implements Serializable {
    private long personId;
    private String personName;
    private long creationDate;
    private boolean isBlocked;
    private byte gender;
    private long birthday;
    private int countryId;
    private int cityId;
    private List<PersonOwnAccount> personOwnAccounts;
    private List<PersonInvestCompany> personInvestCompanies;
    private final LinkedHashSet<PersonGuaranteePerson> guaranteeSrc;
    private final LinkedHashSet<PersonGuaranteePerson> guaranteeDst;
    private List<PersonApplyLoan> personApplyLoans;

    public Person() {
        personOwnAccounts = new LinkedList<>();
        personInvestCompanies = new LinkedList<>();
        guaranteeSrc = new LinkedHashSet<>();
        guaranteeDst = new LinkedHashSet<>();
        personApplyLoans = new LinkedList<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Person) {
            Person person = (Person) obj;
            return person.getPersonId() == this.getPersonId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(personId);
    }

    public boolean canGuarantee(Person to) {
        // can not: equal, guarantee the same person twice, guarantee cyclically
        return !this.equals(to) && !guaranteeSrc.contains(to) && !guaranteeDst.contains(to);
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

    public HashSet<PersonGuaranteePerson> getGuaranteeSrc() {
        return guaranteeSrc;
    }

    public HashSet<PersonGuaranteePerson> getGuaranteeDst() {
        return guaranteeDst;
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

    public String getGender() {
        return (gender == (byte) 1) ? "male" : "female";
    }

    public void setGender(byte gender) {
        this.gender = gender;
    }

    public long getBirthday() {
        return birthday;
    }

    public void setBirthday(long birthday) {
        this.birthday = birthday;
    }


    public int getCountryId() {
        return countryId;
    }

    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }

    public String getCountryName() {
        return Dictionaries.places.getPlaceName(countryId);
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return Dictionaries.places.getPlaceName(cityId);
    }
}
