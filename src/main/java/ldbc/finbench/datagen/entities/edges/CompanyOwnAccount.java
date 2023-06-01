package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;

public class CompanyOwnAccount implements DynamicActivity, Serializable {
    private Company company;
    private Account account;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public CompanyOwnAccount(Company company, Account account, long creationDate, long deletionDate,
                             boolean isExplicitlyDeleted) {
        this.company = company;
        this.account = account;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createCompanyOwnAccount(Company company, Account account, long creationDate) {
        CompanyOwnAccount companyOwnAccount =
            new CompanyOwnAccount(company, account, creationDate, account.getDeletionDate(),
                                  account.isExplicitlyDeleted());
        company.getAccounts().add(account);
        company.getCompanyOwnAccounts().add(companyOwnAccount);
        account.setOwnerType(PersonOrCompany.COMPANY);
        account.setCompanyOwner(company);
    }

    public Company getCompany() {
        return company;
    }

    public void setCompany(Company company) {
        this.company = company;
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(Account account) {
        this.account = account;
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
