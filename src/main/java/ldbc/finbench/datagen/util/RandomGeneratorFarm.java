package ldbc.finbench.datagen.util;

import java.io.Serializable;
import java.util.Random;
import ldbc.finbench.datagen.generation.DatagenParams;

public class RandomGeneratorFarm implements Serializable {
    private final int numRandomGenerators;
    private final Random[] randomGenerators;

    public enum Aspect {
        // vertex: person
        PERSON_NAME,
        PERSON_DATE,
        GENDER,
        PERSON_BIRTHDAY,
        PERSON_COUNTRY,
        PERSON_CITY,

        // vertex: company
        COMPANY_NAME,
        COMPANY_DATE,
        COMPANY_COUNTRY,
        COMPANY_CITY,
        COMPANY_BUSINESS,
        COMPANY_DESCRIPTION,
        COMPANY_URL,

        // vertex: loan
        LOAN_AMOUNT,
        LOAN_SUBEVENTS_DATE,
        LOAN_INTEREST_RATE,
        LOAN_USAGE,

        // vertex: medium
        MEDIUM_NAME,
        MEDIUM_RISK_LEVEL,
        MEDUIM_LAST_LOGIN_DATE,

        // vertex: account
        ACCOUNT_TYPE,
        ACCOUNT_CREATION_DATE,
        ACCOUNT_DELETE_DATE,
        ACCOUNT_OWNER_TYPE,
        ACCOUNT_NICKNAME,
        ACCOUNT_TOP_EMAIL,
        ACCOUNT_EMAIL,
        ACCOUNT_PHONENUM,
        ACCOUNT_FREQ_LOGIN_TYPE,
        ACCOUNT_LAST_LOGIN_TIME,
        ACCOUNT_LEVEL,
        DELETE_ACCOUNT,

        // edge: person own account
        NUM_ACCOUNTS_PER_PERSON,

        // edge: company own account
        NUM_ACCOUNTS_PER_COMPANY,

        // edge: transfer
        TRANSFER_DATE,
        TRANSFER_ORDERNUM,
        TRANSFER_COMMENT,
        TRANSFER_PAYTYPE,
        TRANSFER_GOODSTYPE,

        // edge: withdraw
        ACCOUNT_WHETHER_WITHDRAW,
        WITHDRAW_DATE,

        // edge: signin
        SIGNIN_DATE,
        SIGNIN_COUNTRY,
        SIGNIN_CITY,

        // edge: person own account
        PERSON_OWN_ACCOUNT_DATE,
        // edge: company own account
        COMPANY_OWN_ACCOUNT_DATE,

        // edge: person invest
        PERSON_INVEST_DATE,
        // edge: company invest
        COMPANY_INVEST_DATE,
        INVEST_RATIO,

        // edge: person guarantee
        PICK_PERSON_GUARANTEE,
        NUM_GUARANTEES_PER_PERSON,
        PERSON_GUARANTEE_DATE,
        PERSON_GUARANTEE_RELATIONSHIP,
        // edge: company guarantee
        PICK_COMPANY_GUARANTEE,
        NUM_GUARANTEES_PER_COMPANY,
        COMPANY_GUARANTEE_DATE,

        // edge: person loan
        PICK_PERSON_FOR_LOAN,
        NUM_LOANS_PER_PERSON,
        PERSON_APPLY_LOAN_ORGANIZATION,
        PERSON_APPLY_LOAN_DATE,
        // edge: company loan
        PICK_COMPANY_FOR_LOAN,
        NUM_LOANS_PER_COMPANY,
        COMPANY_APPLY_LOAN_DATE,
        COMPANY_APPLY_LOAN_ORGANIZATION,

        UNIFORM,
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
