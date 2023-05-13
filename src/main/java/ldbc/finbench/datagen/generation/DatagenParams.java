package ldbc.finbench.datagen.generation;

import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.generation.distribution.PowerLawBucketsDistribution;
import ldbc.finbench.datagen.generation.distribution.PowerLawFormulaDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;

public class DatagenParams {
    public static final String DICTIONARY_DIRECTORY = "/dictionaries/";
    public static final String companyNameFile = DICTIONARY_DIRECTORY + "companyNames.txt";
    public static final String personSurnameFile = DICTIONARY_DIRECTORY + "surnames.txt";
    public static final String mediumNameFile = DICTIONARY_DIRECTORY + "mediumNames.txt";
    public static final String accountFile = DICTIONARY_DIRECTORY + "accountTypes.txt";

    public static final String DISTRIBUTION_DIRECTORY = "/distributions/";
    public static final String fbPowerlawDegreeFile = DISTRIBUTION_DIRECTORY + "facebookPowerlawBucket.dat";
    public static final String hourDistributionFile = DISTRIBUTION_DIRECTORY + "hourDistribution.dat";
    public static final String accountDeleteFile = DISTRIBUTION_DIRECTORY + "accountDelete.txt";
    public static final String powerLawActivityDeleteFile = DISTRIBUTION_DIRECTORY + "powerLawActivityDeleteDate.txt";
    public static final String inDegreeRegressionFile = DISTRIBUTION_DIRECTORY + "inDegreeRegression.txt";
    public static final String outDegreeRegressionFile = DISTRIBUTION_DIRECTORY + "outDegreeRegression.txt";
    public static final String multiplictyPowerlawRegressionFile =
        DISTRIBUTION_DIRECTORY + "multiplicityPowerlawRegression.txt";
    public static int blockSize = 0;
    public static String degreeDistribution;
    public static String multiplicityDistribution;
    public static int delta = 0;
    public static long minNumDegree = 0;
    public static long maxNumDegree = 0;
    public static int minMultiplicity = 0;
    public static int maxMultiplicity = 0;
    public static double blockedAccountRatio = 0.0;
    public static int numUpdateStreams = 0;
    public static long numPersons = 0;
    public static long numCompanies = 0;
    public static long numMediums = 0;
    public static int numYears = 0;
    public static String outputDir;
    public static int startYear = 0;
    public static double companyInvestedByPersonFraction = 0.0;
    public static double companyHasWorkerFraction = 0.0;
    public static double accountSignedInFraction = 0.0;
    public static double baseProbCorrelated = 0.0;
    public static double limitProCorrelated = 0.0;


    public static void readConf(GeneratorConfiguration conf) {
        try {
            blockSize = intConf(conf, "generator.blockSize");
            degreeDistribution = stringConf(conf, "generator.degreeDistribution");
            multiplicityDistribution = stringConf(conf, "generator.multiplicityDistribution");
            delta = intConf(conf, "generator.deleteDelta");
            minNumDegree = longConf(conf, "generator.minNumDegree");
            maxNumDegree = longConf(conf, "generator.maxNumDegree");
            minMultiplicity = intConf(conf, "generator.minMultiplicity");
            maxMultiplicity = intConf(conf, "generator.maxMultiplicity");
            blockedAccountRatio = doubleConf(conf, "generator.blockedAccountRatio");
            numUpdateStreams = intConf(conf, "generator.mode.interactive.numUpdateStreams");
            numPersons = longConf(conf, "generator.numPersons");
            numCompanies = longConf(conf, "generator.numCompanies");
            numMediums = longConf(conf, "generator.numMediums");
            outputDir = stringConf(conf, "generator.outputDir");
            startYear = intConf(conf, "generator.startYear");
            numYears = intConf(conf, "generator.numYears");
            companyInvestedByPersonFraction = doubleConf(conf, "generator.companyInvestedByPersonFraction");
            companyHasWorkerFraction = doubleConf(conf, "generator.companyHasWorkerFraction");
            accountSignedInFraction = doubleConf(conf, "generator.accountSignedInFraction");
            baseProbCorrelated = doubleConf(conf, "generator.baseProbCorrelated");
            limitProCorrelated = doubleConf(conf, "generator.limitProCorrelated");

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
        if (degreeDistribution.equals("powerlaw")) {
            return new PowerLawFormulaDistribution(DatagenParams.inDegreeRegressionFile, DatagenParams.minNumDegree,
                                                   DatagenParams.maxNumDegree);
        } else if (degreeDistribution.equals("powerlawbucket")) {
            return new PowerLawBucketsDistribution();
        } else {
            throw new IllegalStateException("Unexpected inDegree distribution: " + degreeDistribution);
        }
    }

    public static DegreeDistribution getOutDegreeDistribution() {
        if (degreeDistribution.equals("powerlaw")) {
            return new PowerLawFormulaDistribution(DatagenParams.outDegreeRegressionFile, DatagenParams.minNumDegree,
                                                   DatagenParams.maxNumDegree);
        } else if (degreeDistribution.equals("powerlawbucket")) {
            return new PowerLawBucketsDistribution();
        } else {
            throw new IllegalStateException("Unexpected outDegree distribution: " + degreeDistribution);
        }
    }

    public static DegreeDistribution getMultiplicityDistribution() {
        if (multiplicityDistribution.equals("powerlaw")) {
            return new PowerLawFormulaDistribution(DatagenParams.multiplictyPowerlawRegressionFile,
                                                   DatagenParams.minMultiplicity, DatagenParams.maxMultiplicity);
        } else if (multiplicityDistribution.equals("powerlawbucket")) {
            return new PowerLawBucketsDistribution();
        } else {
            throw new IllegalStateException("Unexpected multiplicty distribution: " + multiplicityDistribution);
        }
    }
}
