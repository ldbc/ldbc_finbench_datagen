package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyApplyLoan implements DynamicActivity, Serializable {
    private final Company company;
    private final Loan loan;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String organization;
    private final String comment;

    public CompanyApplyLoan(Company company, Loan loan, long creationDate, long deletionDate,
                            boolean isExplicitlyDeleted, String organization, String comment) {
        this.company = company;
        this.loan = loan;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.organization = organization;
        this.comment = comment;
    }

    public static void createCompanyApplyLoan(RandomGeneratorFarm farm, long creationDate, Company company, Loan loan) {
        String organization = Dictionaries.loanOrganizations.getUniformDistRandomText(
            farm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_ORGANIZATION));
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        loan.setOwnerType(PersonOrCompany.COMPANY);
        loan.setOwnerCompany(company);
        CompanyApplyLoan companyApplyLoan =
            new CompanyApplyLoan(company, loan, creationDate, 0, false, organization, comment);
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

    public String getComment() {
        return comment;
    }
}
