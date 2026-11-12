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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
        // HashMap instead of ConcurrentHashMap — no concurrency needed (single partition context)
        multiplicityMap = new HashMap<>();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        multiplicityDist.reset(seed);
        randIndex.setSeed(seed);
    }

    // Shift-based remove on int[] that matches ArrayList<Integer>.remove(index) behavior exactly.
    // This preserves the original sequential scanning order and identical output.
    private static void shiftRemove(int[] arr, int size, int index) {
        System.arraycopy(arr, index + 1, arr, index, size - index - 1);
    }

    // Generation to parts will mess up the average degree(make it bigger than expected) caused by ceiling operations.
    // Also, it will mess up the long tail range of powerlaw distribution of degrees caused by 1 rounded to 2.
    // See the plot drawn by check_transfer.py for more details.
    //
    // Memory optimizations (output-identical to original):
    // 1. int[] + shiftRemove replaces ArrayList<Integer> — avoids Integer boxing (16 bytes per element → 4 bytes)
    // 2. HashMap replaces ConcurrentHashMap — avoids concurrent overhead (no concurrency needed)
    // 3. Algorithm logic is preserved exactly: same sequential scan, same termination, same random generators
    public List<Account> accountActivities(Account[] accounts, WithdrawCard[] cards, int blockId) {
        return accountActivities(accounts, accounts, cards, blockId);
    }

    public List<Account> accountActivities(Account[] accounts, Account[] transferTargets, WithdrawCard[] cards,
                                           int blockId) {
        resetState(blockId);
        Random pickAccountForWithdrawal = randomFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_WHETHER_WITHDRAW);

        int accountSize = accounts.length;
        int targetSize = transferTargets.length;
        // Use primitive int[] instead of ArrayList<Integer> to avoid boxing overhead.
        // shiftRemove preserves the same element ordering as ArrayList.remove(index).
        int[] availableToAccountIds = new int[targetSize];
        int availableSize = targetSize;
        for (int i = 0; i < targetSize; i++) {
            availableToAccountIds[i] = i;
        }
        int localMaxSkippedCount = Math.min(maxSkippedCount, (int) (skippedRatio * targetSize));

        int cardsize = cards.length;
        for (int fromIndex = 0; fromIndex < accountSize; fromIndex++) {
            Account from = accounts[fromIndex];
            // TRANSFER: account transfer to other accounts
            while (from.getAvailableOutDegree() != 0 && availableSize > 0) {
                int skippedCount = 0;
                for (int j = 0; j < availableSize; j++) {
                    int toIndex = availableToAccountIds[j];
                    Account to = transferTargets[toIndex];
                    if (cannotTransfer(from, to)) {
                        skippedCount++;
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDist.nextDegree(),
                                                 Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree()));
                    for (int mindex = 0; mindex < numTransfers; mindex++) {
                        Transfer.createTransfer(randomFarm, from, to, mindex);
                    }

                    if (to.getAvailableInDegree() == 0) {
                        // Shift-remove preserves element ordering (identical to original ArrayList behavior)
                        shiftRemove(availableToAccountIds, availableSize, j);
                        availableSize--;
                        j--;
                    }
                    if (from.getAvailableOutDegree() == 0) {
                        break;
                    }
                }
                if (skippedCount >= Math.min(localMaxSkippedCount, availableSize)) {
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
        return Arrays.asList(accounts);
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
