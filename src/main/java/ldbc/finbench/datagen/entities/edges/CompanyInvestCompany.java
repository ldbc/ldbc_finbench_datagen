package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class CompanyInvestCompany implements DynamicActivity, Serializable {
    private long fromCompanyId;
    private long toCompanyId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public CompanyInvestCompany(long fromCompanyId, long toCompanyId,
                                long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.fromCompanyId = fromCompanyId;
        this.toCompanyId = toCompanyId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static CompanyInvestCompany createCompanyInvestCompany(Random random,
                                                                  Company fromCompany, Company toCompany) {
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(random, fromCompany, toCompany);

        CompanyInvestCompany companyInvestCompany = new CompanyInvestCompany(fromCompany.getCompanyId(),
                toCompany.getCompanyId(), creationDate, 0, false);
        fromCompany.getCompanyInvestCompanies().add(companyInvestCompany);

        return companyInvestCompany;
    }

    public long getFromCompanyId() {
        return fromCompanyId;
    }

    public void setFromCompanyId(long fromCompanyId) {
        this.fromCompanyId = fromCompanyId;
    }

    public long getToCompanyId() {
        return toCompanyId;
    }

    public void setToCompanyId(long toCompanyId) {
        this.toCompanyId = toCompanyId;
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
