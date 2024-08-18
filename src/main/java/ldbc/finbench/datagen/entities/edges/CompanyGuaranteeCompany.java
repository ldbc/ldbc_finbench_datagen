package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class CompanyGuaranteeCompany implements DynamicActivity, Serializable {
    private final Company fromCompany;
    private final Company toCompany;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String relationship;

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
                                                                                      toCompany, creationDate, 0, false,
                                                                                      "business associate");
        fromCompany.getGuaranteeSrc().add(companyGuaranteeCompany);
        toCompany.getGuaranteeDst().add(companyGuaranteeCompany);
    }

    public Company getFromCompany() {
        return fromCompany;
    }

    public Company getToCompany() {
        return toCompany;
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

    public String getRelationship() {
        return relationship;
    }
}
