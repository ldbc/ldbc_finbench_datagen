package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;

public class CompanyGuaranteeCompany implements DynamicActivity, Serializable {
    private long fromCompanyId;
    private long toCompanyId;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public CompanyGuaranteeCompany(long fromCompanyId, long toCompanyId,
                                   long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.fromCompanyId = fromCompanyId;
        this.toCompanyId = toCompanyId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static CompanyGuaranteeCompany createCompanyGuaranteeCompany(Random random,
                                                                        Company fromCompany, Company toCompany) {
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(random, fromCompany, toCompany);

        CompanyGuaranteeCompany companyGuaranteeCompany = new CompanyGuaranteeCompany(fromCompany.getCompanyId(),
                toCompany.getCompanyId(), creationDate, 0, false);
        fromCompany.getCompanyGuaranteeCompanies().add(companyGuaranteeCompany);

        return companyGuaranteeCompany;
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
