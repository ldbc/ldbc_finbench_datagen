package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class CompanyInvestCompany implements DynamicActivity, Serializable {
    private Company fromCompany;
    private Company toCompany;
    private double ratio;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public CompanyInvestCompany(Company fromCompany, Company toCompany,
                                long creationDate, long deletionDate, double ratio, boolean isExplicitlyDeleted) {
        this.fromCompany = fromCompany;
        this.toCompany = toCompany;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.ratio = ratio;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static CompanyInvestCompany createCompanyInvestCompany(Random dateRandom, Random ratioRandom,
                                                                  Company fromCompany, Company toCompany) {
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(dateRandom, fromCompany, toCompany);
        double ratio = ratioRandom.nextDouble();
        CompanyInvestCompany companyInvestCompany =
            new CompanyInvestCompany(fromCompany, toCompany, creationDate, 0, ratio, false);
        fromCompany.getCompanyInvestCompanies().add(companyInvestCompany);

        return companyInvestCompany;
    }

    public void scaleRatio(double sum) {
        this.ratio = this.ratio / sum;
    }

    public double getRatio() {
        return ratio;
    }

    public void setRatio(double ratio) {
        this.ratio = ratio;
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
}
