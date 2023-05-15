package ldbc.finbench.datagen.generation;

import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.generation.distribution.PowerLawBucketsDistribution;
import ldbc.finbench.datagen.generation.distribution.PowerLawFormulaDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;

public class DatagenParams {
    public static final String DICTIONARY_DIR = "/dictionaries/";
    public static final String companyNameFile = DICTIONARY_DIR + "companyNames.txt";
    public static final String personSurnameFile = DICTIONARY_DIR + "surnames.txt";
    public static final String mediumNameFile = DICTIONARY_DIR + "mediumNames.txt";
    public static final String accountFile = DICTIONARY_DIR + "accountTypes.txt";
    public static final String DISTRIBUTION_DIR = "/distributions/";
    public static final String fbPowerlawDegreeFile = DISTRIBUTION_DIR + "facebookPowerlawBucket.dat";
    public static final String hourDistributionFile = DISTRIBUTION_DIR + "hourDistribution.dat";
    public static final String accountDeleteFile = DISTRIBUTION_DIR + "accountDelete.txt";
    public static final String powerLawActivityDeleteFile = DISTRIBUTION_DIR + "powerLawActivityDeleteDate.txt";
    public static final String inDegreeRegressionFile = DISTRIBUTION_DIR + "inDegreeRegression.txt";
    public static final String outDegreeRegressionFile = DISTRIBUTION_DIR + "outDegreeRegression.txt";
    public static final String multiplictyPowerlawRegFile = DISTRIBUTION_DIR + "multiplicityPowerlawRegression.txt";

    public static int defaultSeed = 0;
    public static String outputDir;
    public static int startYear = 0;
    public static int numYears = 0;
    public static int blockSize = 0;
    public static long deleteDelta = 0;
    public static long activityDelta = 0;
    public static int maxAccountsPerOwner = 0;
    public static String tsfDegreeDistribution;
    public static String tsfMultiplicityDistribution;
    public static long tsfMinNumDegree = 0;
    public static long tsfMaxNumDegree = 0;
    public static int tsfMinMultiplicity = 0;
    public static int tsfMaxMultiplicity = 0;
    public static double blockedAccountRatio = 0.0;
    public static double blockedMediumRatio = 0.0;
    public static int numUpdateStreams = 0;
    public static long numPersons = 0;
    public static long numCompanies = 0;
    public static long numMediums = 0;
    public static double companyInvestedFraction = 0.0;
    public static int minInvestors = 0;
    public static int maxInvestors = 0;
    public static double companyHasWorkerFraction = 0.0;
    public static double accountSignedInFraction = 0.0;
    public static int maxSignInPerPair = 0;
    public static int maxAccountToSignIn = 0;
    public static double personGuaranteeFraction = 0.0;
    public static double companyGuaranteeFraction = 0.0;
    public static int maxTargetsToGuarantee = 0;
    public static double personLoanFraction = 0.0;
    public static double companyLoanFraction = 0.0;
    public static int maxLoans = 0;
    public static long minLoanAmount = 0;
    public static long maxLoanAmount = 0;
    public static double baseProbCorrelated = 0.0;
    public static double limitProCorrelated = 0.0;


    public static void readConf(GeneratorConfiguration conf) {
        try {
            blockSize = intConf(conf, "spark.blockSize");
            defaultSeed = intConf(conf, "generator.defaultSeed");
            numUpdateStreams = intConf(conf, "generator.numUpdateStreams");
            numPersons = longConf(conf, "generator.numPersons");
            numCompanies = longConf(conf, "generator.numCompanies");
            numMediums = longConf(conf, "generator.numMediums");
            outputDir = stringConf(conf, "generator.outputDir");
            startYear = intConf(conf, "generator.startYear");
            numYears = intConf(conf, "generator.numYears");
            deleteDelta = longConf(conf, "generator.deleteDelta"); // 10h by default
            activityDelta = longConf(conf, "generator.activityDelta"); // 3600s/1h by default

            blockedAccountRatio = doubleConf(conf, "account.blockedAccountRatio");

            blockedMediumRatio = doubleConf(conf, "medium.blockedMediumRatio");

            maxAccountsPerOwner = intConf(conf, "own.maxAccounts");

            tsfDegreeDistribution = stringConf(conf, "transfer.degreeDistribution");
            tsfMinNumDegree = longConf(conf, "transfer.minNumDegree");
            tsfMaxNumDegree = longConf(conf, "transfer.maxNumDegree");
            tsfMultiplicityDistribution = stringConf(conf, "transfer.multiplicityDistribution");
            tsfMinMultiplicity = intConf(conf, "transfer.minMultiplicity");
            tsfMaxMultiplicity = intConf(conf, "transfer.maxMultiplicity");
            baseProbCorrelated = doubleConf(conf, "transfer.baseProbCorrelated");
            limitProCorrelated = doubleConf(conf, "transfer.limitProCorrelated");

            accountSignedInFraction = doubleConf(conf, "signIn.accountSignedInFraction");
            maxSignInPerPair = intConf(conf, "signIn.maxMultiplicity");
            maxAccountToSignIn = intConf(conf, "signIn.maxAccountToSignIn");

            personGuaranteeFraction = doubleConf(conf, "guarantee.personGuaranteeFraction");
            companyGuaranteeFraction = doubleConf(conf, "guarantee.companyGuaranteeFraction");
            maxTargetsToGuarantee = intConf(conf, "guarantee.maxTargetsToGuarantee");

            personLoanFraction = doubleConf(conf, "loan.personLoanFraction");
            companyLoanFraction = doubleConf(conf, "loan.companyLoanFraction");
            maxLoans = intConf(conf, "loan.maxLoans");
            minLoanAmount = longConf(conf, "loan.minLoanAmount");
            maxLoanAmount = longConf(conf, "loan.maxLoanAmount");

            companyInvestedFraction = doubleConf(conf, "invest.companyInvestedFraction");
            minInvestors = intConf(conf, "invest.minInvestors");
            maxInvestors = intConf(conf, "invest.maxInvestors");

            System.out.println(" ... Num Accounts " + (numPersons + numCompanies));
            System.out.println(" ... Start Year " + startYear);
            System.out.println(" ... Num Years " + numYears);
        } catch (Exception e) {
            System.out.println("Error reading scale factors or conf");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static Integer intConf(GeneratorConfiguration conf, String param) {
        return Integer.parseInt(conf.get(param));
    }

    private static Long longConf(GeneratorConfiguration conf, String param) {
        return Long.parseLong(conf.get(param));
    }

    private static Double doubleConf(GeneratorConfiguration conf, String param) {
        return Double.parseDouble(conf.get(param));
    }

    private static String stringConf(GeneratorConfiguration conf, String param) {
        return conf.get(param);
    }

    private static double scale(long numPersons, double mean) {
        return Math.log10(mean * numPersons / 2 + numPersons);
    }

    public static DegreeDistribution getInDegreeDistribution() {
        if (tsfDegreeDistribution.equals("powerlaw")) {
            return new PowerLawFormulaDistribution(DatagenParams.inDegreeRegressionFile, DatagenParams.tsfMinNumDegree,
                                                   DatagenParams.tsfMaxNumDegree);
        } else if (tsfDegreeDistribution.equals("powerlawbucket")) {
            return new PowerLawBucketsDistribution();
        } else {
            throw new IllegalStateException("Unexpected inDegree distribution: " + tsfDegreeDistribution);
        }
    }

    public static DegreeDistribution getOutDegreeDistribution() {
        if (tsfDegreeDistribution.equals("powerlaw")) {
            return new PowerLawFormulaDistribution(DatagenParams.outDegreeRegressionFile, DatagenParams.tsfMinNumDegree,
                                                   DatagenParams.tsfMaxNumDegree);
        } else if (tsfDegreeDistribution.equals("powerlawbucket")) {
            return new PowerLawBucketsDistribution();
        } else {
            throw new IllegalStateException("Unexpected outDegree distribution: " + tsfDegreeDistribution);
        }
    }

    public static DegreeDistribution getTsfMultiplicityDistribution() {
        if (tsfMultiplicityDistribution.equals("powerlaw")) {
            return new PowerLawFormulaDistribution(DatagenParams.multiplictyPowerlawRegFile,
                                                   DatagenParams.tsfMinMultiplicity, DatagenParams.tsfMaxMultiplicity);
        } else if (tsfMultiplicityDistribution.equals("powerlawbucket")) {
            return new PowerLawBucketsDistribution();
        } else {
            throw new IllegalStateException("Unexpected multiplicty distribution: " + tsfMultiplicityDistribution);
        }
    }
}
