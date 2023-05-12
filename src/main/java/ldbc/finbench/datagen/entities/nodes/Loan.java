package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Deposit;

public class Loan implements Serializable {
    private long loanId;
    private double loanAmount;
    private double balance;
    private List<Deposit> deposits;
    private long creationDate;
    private long maxDegree;

    public Loan(long loanId, double loanAmount, double balance, long creationDate, long maxDegree) {
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

    public double getLoanAmount() {
        return loanAmount;
    }

    public void setLoanAmount(double loanAmount) {
        this.loanAmount = loanAmount;
    }

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
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
