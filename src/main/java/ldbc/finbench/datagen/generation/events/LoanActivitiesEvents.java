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

package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import ldbc.finbench.datagen.entities.edges.Deposit;
import ldbc.finbench.datagen.entities.edges.Repay;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.LoanTargetAccount;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class LoanActivitiesEvents implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random indexRandom;
    private final Random actionRandom;
    private final Random amountRandom;
    private final List<Consumer<Loan>> consumers;
    private Account[] targetAccounts;
    private LoanTargetAccount[] targetLoanAccounts;
    private int targetAccountsSize;

    // Note: Don't make it static. It will be accessed by different Spark workers, which makes multiplicity wrong.
    private final Map<String, AtomicLong> multiplicityMap;

    public LoanActivitiesEvents() {
        multiplicityMap = new ConcurrentHashMap<>();
        randomFarm = new RandomGeneratorFarm();
        indexRandom = new Random(DatagenParams.defaultSeed);
        actionRandom = new Random(DatagenParams.defaultSeed);
        amountRandom = new Random(DatagenParams.defaultSeed);
        // Add all defined subevents to the consumers list
        consumers = Arrays.asList(this::depositSubEvent,
                                  this::repaySubEvent,
                                  this::transferSubEvent);
    }

    public void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        indexRandom.setSeed(seed);
        actionRandom.setSeed(seed);
        amountRandom.setSeed(seed);
    }

    public List<Loan> afterLoanApplied(List<Loan> loans, Account[] targets, int blockId) {
        resetState(blockId);
        targetAccounts = targets;
        targetLoanAccounts = null;
        targetAccountsSize = targetAccounts.length;
        for (Loan loan : loans) {
            int count = 0;
            while (count++ < DatagenParams.numLoanActions) {
                Consumer<Loan> consumer = consumers.get(actionRandom.nextInt(consumers.size()));
                consumer.accept(loan);
            }
        }
        return loans;
    }

    // Lightweight overload using LoanTargetAccount[] instead of Account[]
    // This avoids broadcasting full Account objects (with nested edge lists) to all executors.
    public List<Loan> afterLoanApplied(List<Loan> loans, LoanTargetAccount[] targets, int blockId) {
        resetState(blockId);
        targetLoanAccounts = targets;
        targetAccounts = null;
        targetAccountsSize = targetLoanAccounts.length;
        for (Loan loan : loans) {
            int count = 0;
            while (count++ < DatagenParams.numLoanActions) {
                Consumer<Loan> consumer = consumers.get(actionRandom.nextInt(consumers.size()));
                consumer.accept(loan);
            }
        }
        return loans;
    }

    private void depositSubEvent(Loan loan) {
        Account account = getAccount(loan);
        if (!cannotDeposit(loan, account)) {
            double amount = amountRandom.nextDouble() * loan.getBalance();
            Deposit.createDeposit(randomFarm, loan, account, amount);
        }
    }

    private void repaySubEvent(Loan loan) {
        Account account = getAccount(loan);
        if (!cannotRepay(account, loan)) {
            double amount = amountRandom.nextDouble() * (loan.getLoanAmount() - loan.getBalance());
            Repay.createRepay(randomFarm, account, loan, amount);
        }
    }

    public long getMultiplicityIdAndInc(Account from, Account to) {
        String key = from.getAccountId() + "-" + to.getAccountId();
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
    }

    // Lightweight multiplicity for LoanTargetAccount pairs
    public long getMultiplicityIdAndInc(Account from, LoanTargetAccount to) {
        String key = from.getAccountId() + "-" + to.getAccountId();
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
    }

    public long getMultiplicityIdAndInc(LoanTargetAccount from, Account to) {
        String key = from.getAccountId() + "-" + to.getAccountId();
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
    }

    private void transferSubEvent(Loan loan) {
        Account account = getAccount(loan);
        if (targetAccounts != null) {
            // Original path using full Account objects
            Account target = targetAccounts[indexRandom.nextInt(targetAccountsSize)];
            if (actionRandom.nextDouble() < 0.5) {
                if (!cannotTransfer(account, target)) {
                    Transfer.createLoanTransfer(randomFarm, account, target, loan,
                                                getMultiplicityIdAndInc(account, target),
                                                amountRandom.nextDouble() * DatagenParams.transferMaxAmount);
                }
            } else {
                if (!cannotTransfer(target, account)) {
                    Transfer.createLoanTransfer(randomFarm, target, account, loan,
                                                getMultiplicityIdAndInc(target, account),
                                                amountRandom.nextDouble() * DatagenParams.transferMaxAmount);
                }
            }
        } else {
            // Lightweight path using LoanTargetAccount objects
            LoanTargetAccount target = targetLoanAccounts[indexRandom.nextInt(targetAccountsSize)];
            if (actionRandom.nextDouble() < 0.5) {
                if (!cannotTransfer(account, target)) {
                    Transfer.createLoanTransfer(randomFarm, account, target, loan,
                                                getMultiplicityIdAndInc(account, target),
                                                amountRandom.nextDouble() * DatagenParams.transferMaxAmount);
                }
            } else {
                if (!cannotTransfer(target, account)) {
                    Transfer.createLoanTransfer(randomFarm, target, account, loan,
                                                getMultiplicityIdAndInc(target, account),
                                                amountRandom.nextDouble() * DatagenParams.transferMaxAmount);
                }
            }
        }
    }

    public boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }

    // Lightweight overloads using LoanTargetAccount
    public boolean cannotTransfer(Account from, LoanTargetAccount to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }

    public boolean cannotTransfer(LoanTargetAccount from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }

    public boolean cannotDeposit(Loan from, Account to) {
        return from.getBalance() == 0 || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }

    public boolean cannotRepay(Account from, Loan to) {
        return to.getLoanAmount() == to.getBalance()
            || from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta;
    }

    private Account getAccount(Loan loan) {
        Account[] accounts = loan.getAccounts();
        return accounts[indexRandom.nextInt(accounts.length)];
    }
}
