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

    public static void createCompanyInvestCompany(RandomGeneratorFarm farm,
                                                  Company investor, Company target) {
        Random dateRandom = farm.get(RandomGeneratorFarm.Aspect.COMPANY_INVEST_DATE);
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(dateRandom, investor, target);
        double ratio = farm.get(RandomGeneratorFarm.Aspect.INVEST_RATIO).nextDouble();
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        CompanyInvestCompany companyInvestCompany =
            new CompanyInvestCompany(investor, target, creationDate, 0, ratio, false, comment);
        target.getCompanyInvestCompanies().add(companyInvestCompany);
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
