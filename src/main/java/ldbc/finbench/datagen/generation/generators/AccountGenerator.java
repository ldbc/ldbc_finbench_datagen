package ldbc.finbench.datagen.generation.generators;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
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
    private final RandomGeneratorFarm randFarm;
    private final Random blockRandom;

    public AccountGenerator() {
        this.randFarm = new RandomGeneratorFarm();
        this.degreeDistribution = DatagenParams.getInDegreeDistribution();
        this.degreeDistribution.initialize();
        this.accountDeleteDistribution = new AccountDeleteDistribution(DatagenParams.accountDeleteFile);
        this.accountDeleteDistribution.initialize();
        this.blockRandom = new Random(DatagenParams.defaultSeed);
    }

    private int nextId = 0;

    private long composeAccountId(long id, long date, String type, int blockId) {
        // the bits are composed as follows from left to right:
        // 1 bit for sign, 1 bit for type, 14 bits for bucket ranging from 0 to 365 * numYears, 10 bits for blockId,
        // 38 bits for id
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 38);
        // each bucket is 1 day, range from 0 to 365 * numYears
        long bucket = (long) (365 * DatagenParams.numYears * (date - Dictionaries.dates.getSimulationStart())
            / (double) (Dictionaries.dates.getSimulationEnd() - Dictionaries.dates.getSimulationStart()));
        if (type.equals("company")) {
            return (bucket << 48) | (long) blockId << 38 | ((id & idMask));
        } else {
            return 0x1L << 62 | (bucket << 48) | (long) blockId << 38 | ((id & idMask));
        }
    }

    // Note:
    // - maxOutDegree is left as 0 to be assigned by shuffled maxInDegree later
    // - AccountOwnerEnum will be determined when person or company registers its own account
    // TODO: Use the bucket degree distribution instead of using formula and maxDegree in each scale
    public Account generateAccount(long minTime, String personOrCompany, int blockId) {
        Account account = new Account();

        // Set creationDate
        long creationDate =
            Dictionaries.dates.randomAccountCreationDate(randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_CREATION_DATE),
                                                         minTime);
        account.setCreationDate(creationDate);

        // Set accountId
        long accountId = composeAccountId(nextId++, creationDate, personOrCompany, blockId);
        account.setAccountId(accountId);

        // Set inDegree
        long maxInDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.tsfMaxNumDegree);
        account.setMaxInDegree(maxInDegree);
        account.setRawMaxInDegree(maxInDegree);

        // Set deletionDate
        long deletionDate;
        if (accountDeleteDistribution.isDeleted(randFarm.get(RandomGeneratorFarm.Aspect.DELETE_ACCOUNT), maxInDegree)) {
            account.setExplicitlyDeleted(true);
            long maxDeletionDate = Dictionaries.dates.getSimulationEnd();
            deletionDate = Dictionaries.dates.randomAccountDeletionDate(
                randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_DELETE_DATE),
                creationDate, maxDeletionDate);
        } else {
            account.setExplicitlyDeleted(false);
            deletionDate = Dictionaries.dates.getNetworkCollapse();
        }
        account.setDeletionDate(deletionDate);

        // Set outDegree. Note: Leave outDegree as 0 for shuffle later
        account.setMaxOutDegree(0);
        account.setRawMaxOutDegree(0);

        // Set type
        // TODO: the account type should be determined by the type of account owner. Design a ranking function
        String type =
            Dictionaries.accountTypes.getUniformDistRandomText(randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_TYPE));
        account.setType(type);

        // Set nickname
        String nickname = Dictionaries.accountNicknames.getUniformDistRandomText(
            randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_NICKNAME));
        account.setNickname(nickname);

        // Set phonenum
        String phonenum =
            Dictionaries.numbers.generatePhonenum(randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_PHONENUM));
        account.setPhonenum(phonenum);

        // Set email
        String email =
            Dictionaries.emails.getRandomEmail(randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_TOP_EMAIL),
                                               randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_EMAIL));
        account.setEmail(email);

        // Set freqlogintype
        String freqlogintype = Dictionaries.mediumNames.getUniformDistRandomText(
            randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_FREQ_LOGIN_TYPE));
        account.setFreqLoginType(freqlogintype);

        // Set lastLoginTime
        long lastLoginTime = Dictionaries.dates.randomAccountLastLoginTime(
            randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_LAST_LOGIN_TIME), creationDate, deletionDate);
        account.setLastLoginTime(lastLoginTime);

        // Set accountLevel
        String accountLevel =
            Dictionaries.accountLevels.getDistributedText(randFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_LEVEL));
        account.setAccountLevel(accountLevel);

        // Set isBlocked
        account.setBlocked(blockRandom.nextDouble() < DatagenParams.blockedAccountRatio);

        return account;
    }

    public void resetState(long seed) {
        degreeDistribution.reset(seed);
        randFarm.resetRandomGenerators(seed);
        blockRandom.setSeed(seed);
    }
}
