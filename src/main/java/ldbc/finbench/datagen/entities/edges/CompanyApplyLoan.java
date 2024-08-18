package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;

public class CompanyApplyLoan implements DynamicActivity, Serializable {
    private final Company company;
    private final Loan loan;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String organization;

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
        CompanyApplyLoan companyApplyLoan = new CompanyApplyLoan(company, loan, creationDate, 0, false, organization);
        company.getCompanyApplyLoans().add(companyApplyLoan);
    }

    public Company getCompany() {
        return company;
    }

    public Loan getLoan() {
        return loan;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public String getOrganization() {
        return organization;
    }
}
