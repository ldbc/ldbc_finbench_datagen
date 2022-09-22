package ldbc.finbench.datagen.entities.nodes;

public class Loan {
    private long loanId;
    private long loanAmount;
    private long balance;

    public Loan(long loanId, long loanAmount, long balance) {
        this.loanId = loanId;
        this.loanAmount = loanAmount;
        this.balance = balance;
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
}
