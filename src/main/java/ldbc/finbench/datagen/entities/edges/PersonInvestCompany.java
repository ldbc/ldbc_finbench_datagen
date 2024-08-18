package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonInvestCompany implements DynamicActivity, Serializable {
    private final Person person;
    private final Company company;
    private double ratio;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public PersonInvestCompany(Person person, Company company,
                               long creationDate, long deletionDate, double ratio, boolean isExplicitlyDeleted,
                               String comment) {
        this.person = person;
        this.company = company;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.ratio = ratio;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static PersonInvestCompany createPersonInvestCompany(RandomGeneratorFarm farm, Person person,
                                                                Company company) {
        Random dateRandom = farm.get(RandomGeneratorFarm.Aspect.PERSON_INVEST_DATE);
        long creationDate = Dictionaries.dates.randomPersonToCompanyDate(dateRandom, person, company);
        double ratio = farm.get(RandomGeneratorFarm.Aspect.INVEST_RATIO).nextDouble();
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComment(farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        PersonInvestCompany personInvestCompany = new PersonInvestCompany(person, company, creationDate, 0, ratio,
                                                                          false, comment);
        person.getPersonInvestCompanies().add(personInvestCompany);

        return personInvestCompany;
    }

    public void scaleRatio(double sum) {
        this.ratio = this.ratio / sum;
    }

    public double getRatio() {
        return ratio;
    }

    public Company getCompany() {
        return company;
    }

    public Person getPerson() {
        return person;
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
