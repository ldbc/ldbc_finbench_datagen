package ldbc.finbench.datagen.generator.generators;

import java.util.Iterator;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.AccountOwnerEnum;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.AccountTypeDictionary;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.distribution.AccountDeleteDistribution;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class AccountGenerator {
    private final DegreeDistribution degreeDistribution;
    private final AccountDeleteDistribution accountDeleteDistribution;
    private final AccountTypeDictionary accountTypeDictionary;
    private final RandomGeneratorFarm randFarm;
    private final Random blockRandom;
    private final Random personOrCompanyOwnRandom;
    private int nextId = 0;

    public AccountGenerator(GeneratorConfiguration conf) {
        this.randFarm = new RandomGeneratorFarm();
        this.degreeDistribution = DatagenParams.getDegreeDistribution();
        this.accountDeleteDistribution = new AccountDeleteDistribution(DatagenParams.accountDeleteFile);
        this.degreeDistribution.initialize();
        this.accountTypeDictionary = new AccountTypeDictionary();
        this.blockRandom = new Random(0);
        this.personOrCompanyOwnRandom = new Random(0);
    }

    private long composeAccountId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 43);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart())
            / (double) Dictionaries.dates.getSimulationEnd());
        return (bucket << 43) | ((id & idMask));
    }

    public Account generateAccount() {
        Account account = new Account();

        // Set creationDate
        long creationDate = Dictionaries.dates.randomAccountCreationDate(randFarm.get(RandomGeneratorFarm.Aspect.DATE));
        account.setCreationDate(creationDate);

        // Set accountId
        long accountId = composeAccountId(nextId++, creationDate);
        account.setAccountId(accountId);

        // Set inDegree
        // TODO: Use the bucket degree distribution instead of using formula and maxDegree in each scale
        long maxInDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        account.setMaxInDegree(maxInDegree);

        // Set outDegree.
        // Note: Leave outDegree as 0 for shuffle later
        long maxOutDegree = 0;
        account.setMaxOutDegree(maxOutDegree);

        // Set type
        String type =
            Dictionaries.accountTypes.getGeoDistRandomType(randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_TYPE),
                                                           accountTypeDictionary.getNumNames());
        account.setType(type);

        // Set isBlocked
        boolean isBlocked = blockRandom.nextDouble() < DatagenParams.blockedAccountRatio;
        account.setBlocked(isBlocked);

        // Set accountOwnerEnum.
        // Note: Person or Company will be set later
        if (personOrCompanyOwnRandom.nextDouble() < DatagenParams.personCompanyAccountRatio) {
            account.setAccountOwnerEnum(AccountOwnerEnum.PERSON);
        } else {
            account.setAccountOwnerEnum(AccountOwnerEnum.COMPANY);
        }

        // Set deletionDate
        long deletionDate;
        boolean delete =
            accountDeleteDistribution.isDeleted(randFarm.get(RandomGeneratorFarm.Aspect.DELETE_ACCOUNT), maxInDegree);
        if (delete) {
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

    public Iterator<Account> generateAccountBlock(int blockId, int blockSize) {
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
