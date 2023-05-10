package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class CompanyApplyLoan implements DynamicActivity, Serializable {
    private Company company;
    private Loan loan;
    private long loanAmount;
    private long loanBalance;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public CompanyApplyLoan(Company company, Loan loan, long loanAmount, long loanBalance,
                            long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.company = company;
        this.loan = loan;
        this.loanAmount = loanAmount;
        this.loanBalance = loanBalance;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static CompanyApplyLoan createCompanyApplyLoan(Random random, Company company, Loan loan) {
        long creationDate = Dictionaries.dates.randomCompanyToLoanDate(random, company, loan);
        CompanyApplyLoan companyApplyLoan = new CompanyApplyLoan(company, loan,
                loan.getLoanAmount(), loan.getBalance(), creationDate, 0, false);
        company.getCompanyApplyLoans().add(companyApplyLoan);

        return companyApplyLoan;
    }

    public Company getCompany() {
        return company;
    }

    public void setCompany(Company company) {
        this.company = company;
    }

    public Loan getLoan() {
        return loan;
    }

    public void setLoan(Loan loan) {
        this.loan = loan;
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
