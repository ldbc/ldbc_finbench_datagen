package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyInvestCompany implements DynamicActivity, Serializable {
    private final Company fromCompany;
    private final Company toCompany;
    private double ratio;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public CompanyInvestCompany(Company fromCompany, Company toCompany,
                                long creationDate, long deletionDate, double ratio, boolean isExplicitlyDeleted,
                                String comment) {
        this.fromCompany = fromCompany;
        this.toCompany = toCompany;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.ratio = ratio;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static CompanyInvestCompany createCompanyInvestCompany(RandomGeneratorFarm farm,
                                                                  Company fromCompany, Company toCompany) {
        Random dateRandom = farm.get(RandomGeneratorFarm.Aspect.COMPANY_INVEST_DATE);
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(dateRandom, fromCompany, toCompany);
        double ratio = farm.get(RandomGeneratorFarm.Aspect.INVEST_RATIO).nextDouble();
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomText(farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        CompanyInvestCompany companyInvestCompany =
            new CompanyInvestCompany(fromCompany, toCompany, creationDate, 0, ratio, false, comment);
        fromCompany.getCompanyInvestCompanies().add(companyInvestCompany);

        return companyInvestCompany;
    }

    public void scaleRatio(double sum) {
        this.ratio = this.ratio / sum;
    }

    public double getRatio() {
        return ratio;
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

    public String getComment() {
        return comment;
    }
}
