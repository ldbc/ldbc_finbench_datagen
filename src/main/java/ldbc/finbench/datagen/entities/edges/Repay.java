package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class Repay implements DynamicActivity, Serializable {
    private final long accountId;
    private final long loanId;
    private final double amount;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public Repay(Account account, Loan loan, double amount, long creationDate, long deletionDate,
                 boolean isExplicitlyDeleted, String comment) {
        this.accountId = account.getAccountId();
        this.loanId = loan.getLoanId();
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createRepay(RandomGeneratorFarm farm, Account account, Loan loan, double amount) {
        long creationDate =
            Dictionaries.dates.randomAccountToLoanDate(farm.get(RandomGeneratorFarm.Aspect.LOAN_SUBEVENTS_DATE),
                                                       account, loan, account.getDeletionDate());
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        Repay repay = new Repay(account, loan, amount, creationDate, account.getDeletionDate(),
                                account.isExplicitlyDeleted(), comment);
        loan.addRepay(repay);
        //account.getRepays().add(repay);
    }

    public double getAmount() {
        return amount;
    }

    public long getAccountId() {
        return accountId;
    }

    public long getLoanId() {
        return loanId;
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
