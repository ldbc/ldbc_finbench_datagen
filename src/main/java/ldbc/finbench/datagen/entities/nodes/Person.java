/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.ApplyLoan;
import ldbc.finbench.datagen.entities.edges.OwnAccount;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
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
    private final List<OwnAccount> ownAccounts;
    private final List<PersonInvestCompany> personInvestCompanies;
    private final LinkedHashSet<PersonGuaranteePerson> guaranteeSrc;
    private final LinkedHashSet<PersonGuaranteePerson> guaranteeDst;
    private final List<ApplyLoan> applyLoans;
    private final List<Account> accounts;
    private final List<Loan> loans;

    public Person() {
        ownAccounts = new ArrayList<>();
        personInvestCompanies = new LinkedList<>();
        guaranteeSrc = new LinkedHashSet<>();
        guaranteeDst = new LinkedHashSet<>();
        applyLoans = new ArrayList<>();
        accounts = new ArrayList<>();
        loans = new ArrayList<>();
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

    public List<OwnAccount> getOwnAccounts() {
        return ownAccounts;
    }


    public List<Account> getAccount() {
        return accounts;
    }

    public void addAccount(Account account) {
        this.accounts.add(account);
    }

    public List<PersonInvestCompany> getPersonInvestCompanies() {
        return personInvestCompanies;
    }

    public HashSet<PersonGuaranteePerson> getGuaranteeSrc() {
        return guaranteeSrc;
    }

    public HashSet<PersonGuaranteePerson> getGuaranteeDst() {
        return guaranteeDst;
    }

    public List<ApplyLoan> getApplyLoans() {
        return applyLoans;
    }

    public List<Loan> getLoan() {
        return loans;
    }
    
    public void addLoan(Loan loan) {
        this.loans.add(loan);
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

    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }

    public String getCountryName() {
        return Dictionaries.places.getPlaceName(countryId);
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return Dictionaries.places.getPlaceName(cityId);
    }
}
