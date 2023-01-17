package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class CompanyApplyLoan implements DynamicActivity, Serializable {
    private long companyId;
    private long loanId;
    private long loanAmount;
    private long loanBalance;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public CompanyApplyLoan(long companyId, long loanId, long loanAmount, long loanBalance,
                            long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.companyId = companyId;
        this.loanId = loanId;
        this.loanAmount = loanAmount;
        this.loanBalance = loanBalance;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static CompanyApplyLoan createCompanyApplyLoan(Random random, Company company, Loan loan) {
        long creationDate = Dictionaries.dates.randomCompanyToLoanDate(random, company, loan);

        CompanyApplyLoan companyApplyLoan = new CompanyApplyLoan(company.getCompanyId(), loan.getLoanId(),
                loan.getLoanAmount(), loan.getBalance(), creationDate, 0, false);
        company.getCompanyApplyLoans().add(companyApplyLoan);

        return companyApplyLoan;
    }

    public long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(long companyId) {
        this.companyId = companyId;
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

    public long getLoanBalance() {
        return loanBalance;
    }

    public void setLoanBalance(long loanBalance) {
        this.loanBalance = loanBalance;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }
}
