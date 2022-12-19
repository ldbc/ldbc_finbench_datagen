package ldbc.finbench.datagen.generator.generators;

import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.AccountDictionary;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class AccountGenerator {

    private DegreeDistribution degreeDistribution;
    private AccountDictionary accountDictionary;
    private RandomGeneratorFarm randomFarm;
    private int nextId  = 0;

    private long composeAccountId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 43);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 43) | ((id & idMask));
    }

    private Account generateAccount() {

        long creationDate = Dictionaries.dates.randomAccountCreationDate(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        long accountId = composeAccountId(nextId++, creationDate);
        long maxDegree = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumDegree);
        String type = Dictionaries.accountTypes.getGeoDistRandomType(
                randomFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_TYPE),accountDictionary.getNumNames());
        boolean isBlocked = false;

        return new Account(accountId,creationDate,maxDegree,type,isBlocked);
    }
}
