package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class CompanyGuaranteeCompany implements DynamicActivity, Serializable {
    private Company fromCompany;
    private Company toCompany;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;
    private String relationship;

    public CompanyGuaranteeCompany(Company fromCompany, Company toCompany,
                                   long creationDate, long deletionDate, boolean isExplicitlyDeleted, String relation) {
        this.fromCompany = fromCompany;
        this.toCompany = toCompany;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.relationship = relation;
    }

    public static void createCompanyGuaranteeCompany(Random random, Company fromCompany, Company toCompany) {
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(random, fromCompany, toCompany);
        CompanyGuaranteeCompany companyGuaranteeCompany = new CompanyGuaranteeCompany(fromCompany,
                toCompany, creationDate, 0, false, "business associate");
        fromCompany.getGuaranteeSrc().add(companyGuaranteeCompany);
        toCompany.getGuaranteeDst().add(companyGuaranteeCompany);
    }

    public Company getFromCompany() {
        return fromCompany;
    }

    public void setFromCompany(Company fromCompany) {
        this.fromCompany = fromCompany;
    }

    public Company getToCompany() {
        return toCompany;
    }

    public void setToCompany(Company toCompany) {
        this.toCompany = toCompany;
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

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }
}
