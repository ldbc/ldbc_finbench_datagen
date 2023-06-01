package ldbc.finbench.datagen.util;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.generation.DatagenParams;

public class RandomGeneratorFarm implements Serializable {
    private final int numRandomGenerators;
    private final Random[] randomGenerators;

    public enum Aspect {
        PERSON_NAME,
        PERSON_DATE,
        PERSON_OWN_ACCOUNT_DATE,
        PERSON_APPLY_LOAN_DATE,
        PERSON_GUARANTEE_DATE,
        PERSON_INVEST_DATE,
        COMPANY_NAME,
        COMPANY_DATE,
        COMPANY_OWN_ACCOUNT_DATE,
        COMPANY_APPLY_LOAN_DATE,
        COMPANY_GUARANTEE_DATE,
        COMPANY_INVEST_DATE,
        LOAN_AMOUNT,
        LOAN_SUBEVENTS_DATE,
        NUM_LOANS_PER_PERSON,
        NUM_LOANS_PER_COMPANY,
        MEDIUM_NAME,
        SIGNIN_DATE,
        ACCOUNT_TYPE,
        ACCOUNT_CREATION_DATE,
        ACCOUNT_DELETE_DATE,
        ACCOUNT_OWNER_TYPE,
        NUM_ACCOUNTS_PER_PERSON,
        NUM_ACCOUNTS_PER_COMPANY,
        DELETE_ACCOUNT,
        TRANSFER_DATE,
        WITHDRAW_DATE,
        WORKIN_DATE,
        UNIFORM,
        INVEST_RATIO,
        PERSON_WHETHER_GURANTEE,
        COMPANY_WHETHER_GURANTEE,
        PERSON_WHETHER_LOAN,
        COMPANY_WHETHER_LOAN,
        ACCOUNT_WHETHER_WITHDRAW,
        GENDER,
        PERSON_BIRTHDAY,
        PERSON_COUNTRY,
        PERSON_CITY,
        PERSON_APPLY_LOAN_ORGANIZATION,
        PERSON_GUARANTEE_RELATIONSHIP,
        COMPANY_COUNTRY,
        COMPANY_CITY,
        COMPANY_BUSINESS,
        COMPANY_DESCRIPTION,
        COMPANY_URL,
        ACCOUNT_NICKNAME,
        ACCOUNT_TOP_EMAIL,
        ACCOUNT_EMAIL,
        ACCOUNT_PHONENUM,
        ACCOUNT_FREQ_LOGIN_TYPE,
        ACCOUNT_LAST_LOGIN_TIME,
        ACCOUNT_LEVEL,
        LOAN_INTEREST_RATE,
        LOAN_USAGE,
        MEDIUM_RISK_LEVEL,
        MEDUIM_LAST_LOGIN_DATE,
        TRANSFER_ORDERNUM,
        TRANSFER_COMMENT,
        TRANSFER_PAYTYPE,
        TRANSFER_GOODSTYPE,
        SIGNIN_COUNTRY,
        SIGNIN_CITY,
        COMPANY_APPLY_LOAN_ORGANIZATION,
        NUM_ASPECT                  // This must be always the last one.
    }

    public RandomGeneratorFarm() {
        numRandomGenerators = Aspect.values().length;
        randomGenerators = new Random[numRandomGenerators];
        for (int i = 0; i < numRandomGenerators; ++i) {
            randomGenerators[i] = new Random(DatagenParams.defaultSeed);
        }
    }

    public Random get(Aspect aspect) {
        return randomGenerators[aspect.ordinal()];
    }

    public void resetRandomGenerators(long seed) {
        Random seedRandom = new Random(7654321L + 1234567 * seed);
        for (int i = 0; i < numRandomGenerators; i++) {
            randomGenerators[i].setSeed(seedRandom.nextLong());
        }
    }

}
