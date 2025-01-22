/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class Deposit implements DynamicActivity, Serializable {
    private final long loanId;
    private final long accountId;
    private final double amount;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public Deposit(Loan loan, Account account, double amount, long creationDate, long deletionDate,
                   boolean isExplicitlyDeleted, String comment) {
        this.loanId = loan.getLoanId();
        this.accountId = account.getAccountId();
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createDeposit(RandomGeneratorFarm farm, Loan loan, Account account, double amount) {
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
        //account.getDeposits().add(deposit);
    }

    public double getAmount() {
        return amount;
    }

    public long getLoanId() {
        return loanId;
    }

    public long getAccountId() {
        return accountId;
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
