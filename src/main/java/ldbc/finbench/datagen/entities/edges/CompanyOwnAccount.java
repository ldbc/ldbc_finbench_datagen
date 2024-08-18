package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;

public class CompanyOwnAccount implements DynamicActivity, Serializable {
    private final Company company;
    private final Account account;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;

    public CompanyOwnAccount(Company company, Account account, long creationDate, long deletionDate,
                             boolean isExplicitlyDeleted) {
        this.company = company;
        this.account = account;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createCompanyOwnAccount(Company company, Account account, long creationDate) {
        account.setOwnerType(PersonOrCompany.COMPANY);
        account.setCompanyOwner(company);
        CompanyOwnAccount companyOwnAccount =
            new CompanyOwnAccount(company, account, creationDate, account.getDeletionDate(),
                                  account.isExplicitlyDeleted());
        company.getCompanyOwnAccounts().add(companyOwnAccount);
    }

    public Company getCompany() {
        return company;
    }

    public Account getAccount() {
        return account;
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
}
