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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.edges.Withdraw;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class AccountActivitiesEvent implements Serializable {
    public static final class WithdrawCard implements Serializable {
        private final long accountId;
        private final String type;
        private final long creationDate;
        private final long deletionDate;
        private final boolean explicitlyDeleted;

        public WithdrawCard(long accountId, String type, long creationDate, long deletionDate,
                            boolean explicitlyDeleted) {
            this.accountId = accountId;
            this.type = type;
            this.creationDate = creationDate;
            this.deletionDate = deletionDate;
            this.explicitlyDeleted = explicitlyDeleted;
        }

        public long getAccountId() {
            return accountId;
        }

        public String getType() {
            return type;
        }

        public long getCreationDate() {
            return creationDate;
        }

        public long getDeletionDate() {
            return deletionDate;
        }

        public boolean isExplicitlyDeleted() {
            return explicitlyDeleted;
        }
    }

    private final RandomGeneratorFarm randomFarm;
    private final DegreeDistribution multiplicityDist;
    private final Random randIndex;
    private final Map<AccountPair, AtomicLong> multiplicityMap;
    private final float skippedRatio = 0.5f;
    private int maxSkippedCount = 10;

    public AccountActivitiesEvent() {
        randomFarm = new RandomGeneratorFarm();
        multiplicityDist = DatagenParams.getTransferMultiplicityDistribution();
        multiplicityDist.initialize();
        randIndex = new Random(DatagenParams.defaultSeed);
        multiplicityMap = new ConcurrentHashMap<>();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        multiplicityDist.reset(seed);
        randIndex.setSeed(seed);
    }

    private List<Integer> getIndexList(int size) {
        List<Integer> indexList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            indexList.add(i);
        }
        return indexList;
    }

    // Generation to parts will mess up the average degree(make it bigger than expected) caused by ceiling operations.
    // Also, it will mess up the long tail range of powerlaw distribution of degrees caused by 1 rounded to 2.
    // See the plot drawn by check_transfer.py for more details.
    public List<Account> accountActivities(Account[] accounts, WithdrawCard[] cards, int blockId) {
        resetState(blockId);
        Random pickAccountForWithdrawal = randomFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_WHETHER_WITHDRAW);

        int accountSize = accounts.length;
        List<Integer> availableToAccountIds = getIndexList(accountSize);
        maxSkippedCount = Math.min(maxSkippedCount, (int) (skippedRatio * accountSize));

        int cardsize = cards.length;
        // Simplified version of transfer process
        //        for (int i = 0; i < accounts.size(); i++) {
        //            Account from = accounts.get(i);
        //            int skippedCount = 0;
        //            for (int j = i + 1; j < accounts.size(); j++) {
        //                // termination
        //                if (skippedCount >= maxSkippedCount || from.getAvailableOutDegree() == 0) {
        //                    break;
        //                }
        //                Account to = accounts.get(j);
        //                if (j == i || cannotTransfer(from, to)) {
        //                    skippedCount++;
        //                    continue;
        //                }
        //                long numTransfers = Math.min(multiplicityDist.nextDegree(),
        //                                             Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree
        //                                             ()));
        //                for (int mindex = 0; mindex < numTransfers; mindex++) {
        //                    Transfer.createTransfer(randomFarm, from, to, mindex);
        //                }
        //            }
        //        }
        for (int fromIndex = 0; fromIndex < accountSize; fromIndex++) {
            Account from = accounts[fromIndex];
            // TRANSFER: account transfer to other accounts
            while (from.getAvailableOutDegree() != 0) {
                int skippedCount = 0;
                for (int j = 0; j < availableToAccountIds.size(); j++) {
                    int toIndex = availableToAccountIds.get(j);
                    Account to = accounts[toIndex];
                    if (toIndex == fromIndex || cannotTransfer(from, to)) {
                        skippedCount++;
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDist.nextDegree(),
                                                 Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree()));
                    for (int mindex = 0; mindex < numTransfers; mindex++) {
                        Transfer.createTransfer(randomFarm, from, to, mindex);
                    }

                    if (to.getAvailableInDegree() == 0) {
                        availableToAccountIds.remove(j);
                        j--;
                    }
                    if (from.getAvailableOutDegree() == 0) {
                        break;
                    }
                }
                if (skippedCount >= Math.min(maxSkippedCount, availableToAccountIds.size())) {
                    // System.out.println("[Transfer] All accounts skipped for " + from.getAccountId());
                    break;
                }
            }

            // WITHDRAW: account withdraw to cards
            if (cardsize > 0 && pickAccountForWithdrawal.nextDouble() < DatagenParams.accountWithdrawFraction) {
                for (int count = 0; count < DatagenParams.maxWithdrawals; count++) {
                    WithdrawCard to = cards[randIndex.nextInt(cardsize)];
                    if (!cannotWithdraw(from, to)) {
                        Withdraw.createWithdraw(randomFarm,
                                                from,
                                                to.getAccountId(),
                                                to.getType(),
                                                to.getCreationDate(),
                                                to.getDeletionDate(),
                                                to.isExplicitlyDeleted(),
                                                getMultiplicityIdAndInc(from, to.getAccountId()));
                    }
                }
            }
        }
        return java.util.Arrays.asList(accounts);
    }

    // Transfer to self is not allowed
    private boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to) || from.getAvailableOutDegree() == 0 || to.getAvailableInDegree() == 0;
    }

    private boolean cannotWithdraw(Account from, WithdrawCard to) {
        return from.getType().equals("debit card")
            || from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.getAccountId() == to.getAccountId();
    }

    private long getMultiplicityIdAndInc(Account from, long toAccountId) {
        AccountPair key = new AccountPair(from.getAccountId(), toAccountId);
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
    }

    private static final class AccountPair {
        private final long fromAccountId;
        private final long toAccountId;

        private AccountPair(long fromAccountId, long toAccountId) {
            this.fromAccountId = fromAccountId;
            this.toAccountId = toAccountId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof AccountPair)) {
                return false;
            }
            AccountPair other = (AccountPair) obj;
            return fromAccountId == other.fromAccountId && toAccountId == other.toAccountId;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(fromAccountId);
            result = 31 * result + Long.hashCode(toAccountId);
            return result;
        }
    }
}
