package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Deposit;
import ldbc.finbench.datagen.entities.edges.Repay;

public class Loan implements Serializable, Comparable<Loan> {
    private long loanId;
    private double loanAmount;
    private double balance;
    private long creationDate;
    private long maxDegree;
    private String usage;
    private double interestRate;
    private PersonOrCompany ownerType;
    private Person ownerPerson;
    private Company ownerCompany;
    private List<Deposit> deposits;
    private List<Repay> repays;

    public Loan(long loanId, double loanAmount, double balance, long creationDate, long maxDegree,
                String usage, double interestRate) {
        this.loanId = loanId;
        this.loanAmount = loanAmount;
        this.balance = balance;
        this.creationDate = creationDate;
        this.maxDegree = maxDegree;
        this.usage = usage;
        this.interestRate = interestRate;
        deposits = new ArrayList<>();
        repays = new ArrayList<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Loan) {
            Loan loan = (Loan) obj;
            return loanId == loan.getLoanId();
        }
        return false;
    }

    @Override
    public int compareTo(Loan o) {
        return Long.compare(loanId, o.getLoanId());
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

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public PersonOrCompany getOwnerType() {
        return ownerType;
    }

    public void setOwnerType(PersonOrCompany ownerType) {
        this.ownerType = ownerType;
    }

    public long getMaxDegree() {
        return maxDegree;
    }

    public void setMaxDegree(long maxDegree) {
        this.maxDegree = maxDegree;
    }

    public List<Deposit> getDeposits() {
        return deposits;
    }

    public void addDeposit(Deposit deposit) {
        balance -= deposit.getAmount();
        deposits.add(deposit);
    }

    public void addRepay(Repay repay) {
        balance += repay.getAmount();
        repays.add(repay);
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


    public Person getOwnerPerson() {
        return ownerPerson;
    }

    public void setOwnerPerson(Person ownerPerson) {
        this.ownerPerson = ownerPerson;
    }

    public Company getOwnerCompany() {
        return ownerCompany;
    }

    public void setOwnerCompany(Company ownerCompany) {
        this.ownerCompany = ownerCompany;
    }

    public String getUsage() {
        return usage;
    }

    public void setUsage(String usage) {
        this.usage = usage;
    }

    public double getInterestRate() {
        return interestRate;
    }

    public void setInterestRate(double interestRate) {
        this.interestRate = interestRate;
    }
}
