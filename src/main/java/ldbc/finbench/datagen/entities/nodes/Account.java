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
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Deposit;
import ldbc.finbench.datagen.entities.edges.Repay;
import ldbc.finbench.datagen.entities.edges.SignIn;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.edges.Withdraw;


public class Account implements Serializable {
    private long accountId;
    private String type;
    private long creationDate;
    private long deletionDate;
    private String nickname;
    private String phonenum;
    private String email;
    private String freqLoginType;
    private long lastLoginTime;
    private String accountLevel;
    private long maxInDegree; // max in degree before merging
    private long maxOutDegree; // max out degree before merging
    private boolean isBlocked;
    private PersonOrCompany ownerType;
    private Person personOwner;
    private Company companyOwner;
    private boolean isExplicitlyDeleted;
    private final List<Transfer> transferIns;
    private final List<Transfer> transferOuts;
    private final List<Transfer> loanTransfers;
    private final List<Withdraw> withdraws;
    private final List<Deposit> deposits;
    private final List<Repay> repays;
    private final List<SignIn> signIns;

    public Account() {
        transferIns = new ArrayList<>();
        transferOuts = new ArrayList<>();
        loanTransfers = new ArrayList<>();
        withdraws = new ArrayList<>();
        repays = new ArrayList<>();
        deposits = new ArrayList<>();
        signIns = new ArrayList<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Account) {
            Account other = (Account) obj;
            return accountId == other.accountId;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(accountId);
    }

    public long getAvailableInDegree() {
        return Math.max(0, maxInDegree - transferIns.size());
    }

    public long getAvailableOutDegree() {
        return Math.max(0, maxOutDegree - transferOuts.size());
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Transfer> getTransferIns() {
        return transferIns;
    }

    public List<Transfer> getTransferOuts() {
        return transferOuts;
    }

    public List<Transfer> getLoanTransfers() {
        return loanTransfers;
    }

    public List<Withdraw> getWithdraws() {
        return withdraws;
    }

    public List<Deposit> getDeposits() {
        return deposits;
    }

    public List<Repay> getRepays() {
        return repays;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public long getMaxInDegree() {
        return maxInDegree;
    }

    public void setMaxInDegree(long maxInDegree) {
        this.maxInDegree = maxInDegree;
    }

    public long getMaxOutDegree() {
        return maxOutDegree;
    }

    public void setMaxOutDegree(long maxOutDegree) {
        this.maxOutDegree = maxOutDegree;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public PersonOrCompany getOwnerType() {
        return ownerType;
    }

    public void setOwnerType(PersonOrCompany personOrCompany) {
        this.ownerType = personOrCompany;
    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public void setPersonOwner(Person personOwner) {
        this.personOwner = personOwner;
    }

    public void setCompanyOwner(Company companyOwner) {
        this.companyOwner = companyOwner;
    }

    public List<SignIn> getSignIns() {
        return signIns;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public String getPhonenum() {
        return phonenum;
    }

    public void setPhonenum(String phonenum) {
        this.phonenum = phonenum;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFreqLoginType() {
        return freqLoginType;
    }

    public void setFreqLoginType(String freqLoginType) {
        this.freqLoginType = freqLoginType;
    }

    public long getLastLoginTime() {
        return lastLoginTime;
    }

    public void setLastLoginTime(long lastLoginTime) {
        this.lastLoginTime = lastLoginTime;
    }

    public String getAccountLevel() {
        return accountLevel;
    }

    public void setAccountLevel(String accountLevel) {
        this.accountLevel = accountLevel;
    }
}
