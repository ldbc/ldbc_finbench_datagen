package ldbc.finbench.datagen.generation.generators;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.AccountTypeDictionary;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.distribution.AccountDeleteDistribution;
import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

// TODO:
//  To share the degree context across PersonRegister and CompanyRegister to generate the account,
//  this AccountGenerator with its distributions and dictionaries implements Serializable to run
//  in Spark, which may slow the generation.
public class AccountGenerator implements Serializable {
    private final DegreeDistribution degreeDistribution;
    private final AccountDeleteDistribution accountDeleteDistribution;
    private final AccountTypeDictionary accountTypeDictionary;
    private final RandomGeneratorFarm randFarm;
    private final Random blockRandom;
    private int nextId = 0;

    public AccountGenerator() {
        this.randFarm = new RandomGeneratorFarm();
        this.degreeDistribution = DatagenParams.getInDegreeDistribution();
        this.degreeDistribution.initialize();
        this.accountDeleteDistribution = new AccountDeleteDistribution(DatagenParams.accountDeleteFile);
        this.accountDeleteDistribution.initialize();
        this.accountTypeDictionary = new AccountTypeDictionary();
        this.blockRandom = new Random(DatagenParams.defaultSeed);
    }

    private long composeAccountId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 43);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart())
            / (double) Dictionaries.dates.getSimulationEnd());
        return (bucket << 43) | ((id & idMask));
    }

    // Note:
    // - maxOutDegree is left as 0 to be assigned by shuffled maxInDegree later
    // - AccountOwnerEnum will be determined when person or company registers its own account
    // TODO: Use the bucket degree distribution instead of using formula and maxDegree in each scale
    public Account generateAccount() {
        Account account = new Account();

        // Set creationDate
        long creationDate = Dictionaries.dates.randomAccountCreationDate(randFarm.get(RandomGeneratorFarm.Aspect.DATE));
        account.setCreationDate(creationDate);

        // Set accountId
        long accountId = composeAccountId(nextId++, creationDate);
        account.setAccountId(accountId);

        // Set inDegree
        long maxInDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.tsfMaxNumDegree);
        account.setMaxInDegree(maxInDegree);

        // Set outDegree.
        // Note: Leave outDegree as 0 for shuffle later
        long maxOutDegree = 0;
        account.setMaxOutDegree(maxOutDegree);

        // Set type
        // TODO: the account type should be determined by the type of account owner. Design a ranking function
        String type =
            Dictionaries.accountTypes.getUniformDistRandomType(randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_TYPE),
                                                               accountTypeDictionary.getNumNames());
        account.setType(type);

        // Set isBlocked
        account.setBlocked(blockRandom.nextDouble() < DatagenParams.blockedAccountRatio);

        // Set deletionDate
        long deletionDate;
        if (accountDeleteDistribution.isDeleted(randFarm.get(RandomGeneratorFarm.Aspect.DELETE_ACCOUNT), maxInDegree)) {
            account.setExplicitlyDeleted(true);
            long maxDeletionDate = Dictionaries.dates.getSimulationEnd();
            deletionDate = Dictionaries.dates.randomAccountDeletionDate(randFarm.get(RandomGeneratorFarm.Aspect.DATE),
                                                                        creationDate, maxDeletionDate);
        } else {
            account.setExplicitlyDeleted(false);
            deletionDate = Dictionaries.dates.getNetworkCollapse();
        }
        account.setDeletionDate(deletionDate);

        return account;
    }

    public void resetState(long seed) {
        degreeDistribution.reset(seed);
        randFarm.resetRandomGenerators(seed);
        blockRandom.setSeed(seed);
    }

    public Iterator<Account> generateAccountBlock(int blockId, int blockSize) {
        resetState(blockId);
        nextId = blockId * blockSize;
        return new Iterator<Account>() {
            private int accountNum = 0;

            @Override
            public boolean hasNext() {
                return accountNum < blockSize;
            }

            @Override
            public Account next() {
                ++accountNum;
                return generateAccount();
            }
        };
    }
}
