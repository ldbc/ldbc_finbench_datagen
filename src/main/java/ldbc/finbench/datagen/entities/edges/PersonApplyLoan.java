package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonApplyLoan implements DynamicActivity, Serializable {
    private final long personId;
    private final long loanId;
    private final Loan loan; // TODO: can be removed
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String organization;
    private final String comment;
    private final double loanAmount;

    public PersonApplyLoan(Person person, Loan loan, long creationDate, long deletionDate,
                           boolean isExplicitlyDeleted, String organization, String comment) {
        this.personId = person.getPersonId();
        this.loanId = loan.getLoanId();
        this.loan = loan;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.organization = organization;
        this.comment = comment;
        this.loanAmount = loan.getLoanAmount();
    }

    public static void createPersonApplyLoan(RandomGeneratorFarm farm, long creationDate, Person person, Loan loan) {
        String organization = Dictionaries.loanOrganizations.getUniformDistRandomText(
            farm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_ORGANIZATION));
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        loan.setOwnerType(PersonOrCompany.PERSON);
        loan.setOwnerPerson(person);
        PersonApplyLoan personApplyLoan =
            new PersonApplyLoan(person, loan, creationDate, 0, false, organization, comment);
        person.getPersonApplyLoans().add(personApplyLoan);
    }

    public long getPersonId() {
        return personId;
    }

    public long getLoanId() {
        return loanId;
    }

    public Loan getLoan() {
        return loan;
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

    public String getOrganization() {
        return organization;
    }

    public String getComment() {
        return comment;
    }

    public double getLoanAmount() {
        return loanAmount;
    }
}
