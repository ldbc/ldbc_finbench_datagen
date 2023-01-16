package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class CompanyOwnAccount implements DynamicActivity, Serializable {
    private long companyId;
    private long accountId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public CompanyOwnAccount(long companyId, long accountId,
                             long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.companyId = companyId;
        this.accountId = accountId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createCompanyOwnAccount(Random random, Company company, Account account) {
        long creationDate = Dictionaries.dates.randomCompanyToAccountDate(random, company, account);

        company.getCompanyOwnAccounts().add(new CompanyOwnAccount(company.getCompanyId(), account.getAccountId(),
                creationDate, 0, false));
    }

    public long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(long companyId) {
        this.companyId = companyId;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
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
