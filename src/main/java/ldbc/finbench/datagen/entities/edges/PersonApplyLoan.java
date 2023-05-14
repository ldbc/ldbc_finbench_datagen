package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;

public class PersonApplyLoan implements DynamicActivity, Serializable {
    private Person person;
    private Loan loan;
    private long creationDate;
    private long deletionDate;
    private boolean isExplicitlyDeleted;

    public PersonApplyLoan(Person person, Loan loan, long creationDate, long deletionDate,
                           boolean isExplicitlyDeleted) {
        this.person = person;
        this.loan = loan;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static PersonApplyLoan createPersonApplyLoan(long creationDate, Person person, Loan loan) {
        PersonApplyLoan personApplyLoan = new PersonApplyLoan(person, loan, creationDate, 0, false);
        person.getPersonApplyLoans().add(personApplyLoan);
        loan.setOwnerType(PersonOrCompany.PERSON);
        loan.setOwnerPerson(person);

        return personApplyLoan;
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    public Loan getLoan() {
        return loan;
    }

    public void setLoan(Loan loan) {
        this.loan = loan;
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
