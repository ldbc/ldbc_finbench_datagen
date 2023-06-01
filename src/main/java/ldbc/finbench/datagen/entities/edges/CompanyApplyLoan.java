package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class CompanyApplyLoan implements DynamicActivity, Serializable {
    private Company company;
    private Loan loan;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;
    private String organization;

    public CompanyApplyLoan(Company company, Loan loan, long creationDate, long deletionDate,
                            boolean isExplicitlyDeleted, String organization) {
        this.company = company;
        this.loan = loan;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.organization = organization;
    }

    public static void createCompanyApplyLoan(long creationDate, Company company, Loan loan, String organization) {
        loan.setOwnerType(PersonOrCompany.COMPANY);
        loan.setOwnerCompany(company);
        company.getLoans().add(loan);
        CompanyApplyLoan companyApplyLoan = new CompanyApplyLoan(company, loan, creationDate, 0, false, organization);
        company.getCompanyApplyLoans().add(companyApplyLoan);
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

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }
}
