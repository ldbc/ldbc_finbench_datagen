package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Repay;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.edges.Withdraw;


public class Account implements Serializable {
    private long accountId;
    private String type;
    private long creationDate;
    private long deletionDate;
    private long maxInDegree;
    private long maxOutDegree;
    private boolean isBlocked;
    private AccountOwnerEnum accountOwnerEnum;
    private Person personOwner;
    private Company companyOwner;
    private boolean isExplicitlyDeleted;
    private List<Transfer> transfers;
    private List<Withdraw> withdraws;
    private List<Repay> repays;

    public Account() {
        transfers = new ArrayList<>();
        withdraws = new ArrayList<>();
        repays = new ArrayList<>();
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

    public List<Transfer> getTransfers() {
        return transfers;
    }

    public void setTransfers(List<Transfer> transfers) {
        this.transfers = transfers;
    }

    public List<Withdraw> getWithdraws() {
        return withdraws;
    }

    public void setWithdraws(List<Withdraw> withdraws) {
        this.withdraws = withdraws;
    }

    public List<Repay> getRepays() {
        return repays;
    }

    public void setRepays(List<Repay> repays) {
        this.repays = repays;
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


    public AccountOwnerEnum getAccountOwnerEnum() {
        return accountOwnerEnum;
    }

    public void setAccountOwnerEnum(AccountOwnerEnum accountOwnerEnum) {
        this.accountOwnerEnum = accountOwnerEnum;
    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public Person getPersonOwner() {
        return personOwner;
    }

    public void setPersonOwner(Person personOwner) {
        this.personOwner = personOwner;
    }

    public Company getCompanyOwner() {
        return companyOwner;
    }

    public void setCompanyOwner(Company companyOwner) {
        this.companyOwner = companyOwner;
    }
}
