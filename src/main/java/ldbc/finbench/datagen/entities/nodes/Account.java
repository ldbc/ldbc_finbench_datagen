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
    private List<Transfer> transfers;
    private List<Withdraw> withdraws;
    private List<Repay> repays;
    private long creationDate;
    private long maxDegree;
    private boolean isBlocked;

    public Account(long accountId, String type, long creationDate, long maxDegree, boolean isBlocked) {
        this.accountId = accountId;
        this.type = type;
        transfers = new ArrayList<>();
        withdraws = new ArrayList<>();
        repays = new ArrayList<>();
        this.creationDate = creationDate;
        this.maxDegree = maxDegree;
        this.isBlocked = isBlocked;
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

    public long getMaxDegree() {
        return maxDegree;
    }

    public void setMaxDegree(long maxDegree) {
        this.maxDegree = maxDegree;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }
}
