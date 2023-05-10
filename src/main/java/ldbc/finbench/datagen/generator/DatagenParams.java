package ldbc.finbench.datagen.generator;

import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.generator.distribution.PowerLawBucketsDistribution;
import ldbc.finbench.datagen.generator.distribution.PowerLawDegreeDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;

public class DatagenParams {
    public static final String DICTIONARY_DIRECTORY = "/dictionaries/";
    public static final String companyNameFile = DICTIONARY_DIRECTORY + "companies.txt";
    public static final String personSurnameFile = DICTIONARY_DIRECTORY + "surnames.txt";
    public static final String mediumNameFile = DICTIONARY_DIRECTORY + "medium.txt";
    public static final String accountFile = DICTIONARY_DIRECTORY + "accountTypes.txt";

    public static final String DISTRIBUTION_DIRECTORY = "/distributions/";
    public static final String fbPowerlawDegreeFile = DISTRIBUTION_DIRECTORY + "facebookPowerlawBucket.dat";
    public static final String hourDistributionFile = DISTRIBUTION_DIRECTORY + "hourDistribution.dat";
    public static final String accountDeleteFile = DISTRIBUTION_DIRECTORY + "accountDelete.txt";
    public static final String powerLawActivityDeleteFile = DISTRIBUTION_DIRECTORY + "powerLawActivityDeleteDate.txt";
    public static final String inDegreeRegressionFile = DISTRIBUTION_DIRECTORY + "inDegreeRegression.txt";
    public static final String outDegreeRegressionFile = DISTRIBUTION_DIRECTORY + "outDegreeRegression.txt";
    public static int blockSize = 0;
    public static String degreeDistribution;
    public static int delta = 0;
    public static long minNumDegree = 0;
    public static long maxNumDegree = 0;
    public static double blockedAccountRatio = 0.0;
    public static int numUpdateStreams = 0;
    //    public static long numAccounts = 0;
    public static long numPersons = 0;
    public static long numCompanies = 0;
    public static long numMediums = 0;
    public static int numYears = 0;
    public static String outputDir;
    public static int startYear = 0;
    public static double personCompanyAccountRatio = 0.0;


    public static void readConf(GeneratorConfiguration conf) {
        try {
            blockSize = intConf(conf, "generator.blockSize");
            degreeDistribution = stringConf(conf, "generator.degreeDistribution");
            delta = intConf(conf, "generator.deleteDelta");
            minNumDegree = longConf(conf, "generator.minNumDegree");
            maxNumDegree = longConf(conf, "generator.maxNumDegree");
            blockedAccountRatio = doubleConf(conf, "generator.blockedAccountRatio");
            numUpdateStreams = intConf(conf, "generator.mode.interactive.numUpdateStreams");
            //numAccounts = longConf(conf, "generator.numAccounts");
            numPersons = longConf(conf, "generator.numPersons");
            numCompanies = longConf(conf, "generator.numCompanies");
            numMediums = longConf(conf, "generator.numMediums");
            outputDir = stringConf(conf, "generator.outputDir");
            startYear = intConf(conf, "generator.startYear");
            numYears = intConf(conf, "generator.numYears");
            personCompanyAccountRatio = doubleConf(conf, "generator.personCompanyAccountsRatio");

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

    public static DegreeDistribution getDegreeDistribution() {
        if (degreeDistribution.equals("powerlaw")) {
            return new PowerLawDegreeDistribution();
        } else if (degreeDistribution.equals("powerlawbucket")) {
            return new PowerLawBucketsDistribution();
        } else {
            throw new IllegalStateException("Unexpected degree distribution: " + degreeDistribution);
        }
    }
}
