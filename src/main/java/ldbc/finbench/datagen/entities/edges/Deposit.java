package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class Deposit implements DynamicActivity, Serializable {
    private final Loan loan;
    private final Account account;
    private final double amount;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public Deposit(Loan loan, Account account, double amount, long creationDate, long deletionDate,
                   boolean isExplicitlyDeleted, String comment) {
        this.loan = loan;
        this.account = account;
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static Deposit createDeposit(RandomGeneratorFarm farm, Loan loan, Account account, double amount) {
        long creationDate =
            Dictionaries.dates.randomLoanToAccountDate(farm.get(RandomGeneratorFarm.Aspect.LOAN_SUBEVENTS_DATE), loan,
                                                       account, account.getDeletionDate());
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        Deposit deposit =
            new Deposit(loan, account, amount, creationDate, account.getDeletionDate(), account.isExplicitlyDeleted(),
                        comment);
        loan.addDeposit(deposit);
        account.getDeposits().add(deposit);

        return deposit;
    }

    public double getAmount() {
        return amount;
    }

    public Loan getLoan() {
        return loan;
    }

    public Account getAccount() {
        return account;
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
