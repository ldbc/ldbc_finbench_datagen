package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import ldbc.finbench.datagen.entities.edges.Withdraw;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class WithdrawEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;
    private final Map<String, AtomicLong> multiplicityMap;

    public WithdrawEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        multiplicityMap = new ConcurrentHashMap<>();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public long getMultiplicityIdAndInc(Account from, Account to) {
        String key = from.getAccountId() + "-" + to.getAccountId();
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
    }

    public List<Withdraw> withdraw(List<Account> sources, List<Account> cards, int blockId) {
        resetState(blockId);

        List<Withdraw> withdraws = new LinkedList<>();
        for (Account from : sources) {
            int count = 0;
            while (count++ < DatagenParams.maxWithdrawals) {
                Account to = cards.get(randIndex.nextInt(cards.size()));
                if (cannotWithdraw(from, to)) {
                    continue;
                }
                withdraws.add(Withdraw.createWithdraw(randomFarm, from, to, getMultiplicityIdAndInc(from, to)));
            }
        }
        return withdraws;
    }

    public boolean cannotWithdraw(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to);
    }
}
