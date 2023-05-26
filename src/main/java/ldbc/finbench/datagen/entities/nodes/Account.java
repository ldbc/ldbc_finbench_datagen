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
    private long maxInDegree;
    private long maxOutDegree;
    private boolean isBlocked;
    private PersonOrCompany ownerType;
    private Person personOwner;
    private Company companyOwner;
    private boolean isExplicitlyDeleted;
    private List<Transfer> transferIns;
    private List<Transfer> transferOuts;
    private List<Withdraw> withdraws;
    private List<Deposit> deposits;
    private List<Repay> repays;
    private List<SignIn> signIns;

    public Account() {
        transferIns = new ArrayList<>();
        transferOuts = new ArrayList<>();
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

    public long getAvailableInDegree() {
        return maxInDegree - transferIns.size();
    }

    public long getAvailableOutDegree() {
        return maxOutDegree - transferOuts.size();
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

    public void setTransferIns(List<Transfer> transferIns) {
        this.transferIns = transferIns;
    }

    public List<Transfer> getTransferOuts() {
        return transferOuts;
    }

    public void setTransferOuts(List<Transfer> transferIns) {
        this.transferOuts = transferIns;
    }

    public List<Withdraw> getWithdraws() {
        return withdraws;
    }

    public void setWithdraws(List<Withdraw> withdraws) {
        this.withdraws = withdraws;
    }

    public List<Deposit> getDeposits() {
        return deposits;
    }

    public void setDeposits(List<Deposit> deposits) {
        this.deposits = deposits;
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

    public List<SignIn> getSignIns() {
        return signIns;
    }

    public void setSignIns(List<SignIn> signIns) {
        this.signIns = signIns;
    }
}
