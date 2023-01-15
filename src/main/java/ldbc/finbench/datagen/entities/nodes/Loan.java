package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Deposit;

public class Loan implements Serializable {
    private long loanId;
    private long loanAmount;
    private long balance;
    private List<Deposit> deposits;
    private long creationDate;
    private long maxDegree;

    public Loan(long loanId, long loanAmount, long balance,long creationDate, long maxDegree) {
        this.loanId = loanId;
        this.loanAmount = loanAmount;
        this.balance = balance;
        deposits = new ArrayList<>();
        this.creationDate = creationDate;
        this.maxDegree = maxDegree;
    }

    public long getLoanId() {
        return loanId;
    }

    public void setLoanId(long loanId) {
        this.loanId = loanId;
    }

    public long getLoanAmount() {
        return loanAmount;
    }

    public void setLoanAmount(long loanAmount) {
        this.loanAmount = loanAmount;
    }

    public long getBalance() {
        return balance;
    }

    public void setBalance(long balance) {
        this.balance = balance;
    }

    public List<Deposit> getDeposits() {
        return deposits;
    }

    public void setDeposits(List<Deposit> deposits) {
        this.deposits = deposits;
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
}
