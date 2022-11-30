package ldbc.finbench.datagen.generator;

import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.generator.distribution.FacebookDegreeDistribution;
import ldbc.finbench.datagen.generator.distribution.ZipfDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;

public class DatagenParams {
    public static final String DICTIONARY_DIRECTORY = "./resources/datasource/";

    public static final String companyNameFile = DICTIONARY_DIRECTORY + "companies.txt";
    public static final String personSurnameFile = DICTIONARY_DIRECTORY + "surnames.txt";
    public static final String mediumNameFile = DICTIONARY_DIRECTORY + "mediumSample.txt";

    private enum ParameterNames {
        BASE_CORRELATED("generator.baseProbCorrelated"),
        BLOCK_SIZE("generator.blockSize"),
        DEGREE_DISTRIBUTION("generator.degreeDistribution"),
        DELTA("generator.delta"),
        MIN_TEXT_SIZE("generator.minTextSize"),
        MISSING_RATIO("generator.missingRatio"),
        NUM_UPDATE_STREAMS("generator.mode.interactive.numUpdateStreams"),
        NUM_PERSONS("generator.numPersons"),
        NUM_YEARS("generator.numYears"),
        OUTPUT_DIR("generator.outputDir"),
        START_YEAR("generator.startYear");

        private final String name;

        ParameterNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    public static double baseProbCorrelated = 0.0;
    public static int blockSize = 0;
    public static String degreeDistribution;
    public static int delta = 0;
    public static int minTextSize = 0;
    public static double missingRatio = 0.0;
    public static int numUpdateStreams = 0;
    public static long numPersons = 0;
    public static int numYears = 0;
    public static String outputDir;
    public static int startYear = 0;


    public static void readConf(GeneratorConfiguration conf) {
        try {
            ParameterNames[] values = ParameterNames.values();
            for (ParameterNames value : values) {
                if (conf.get(value.toString()) == null) {
                    throw new IllegalStateException("Missing " + value.toString() + " parameter");
                }
            }
            baseProbCorrelated = doubleConf(conf,ParameterNames.BASE_CORRELATED);
            blockSize = intConf(conf,ParameterNames.BLOCK_SIZE);
            degreeDistribution = stringConf(conf,ParameterNames.DEGREE_DISTRIBUTION);
            delta = intConf(conf,ParameterNames.DELTA);
            minTextSize = intConf(conf,ParameterNames.MIN_TEXT_SIZE);
            missingRatio = doubleConf(conf,ParameterNames.MISSING_RATIO);
            numUpdateStreams = intConf(conf,ParameterNames.NUM_UPDATE_STREAMS);
            numPersons = longConf(conf,ParameterNames.NUM_PERSONS);
            numYears = intConf(conf,ParameterNames.NUM_YEARS);
            outputDir = stringConf(conf,ParameterNames.OUTPUT_DIR);
            startYear = intConf(conf,ParameterNames.START_YEAR);

            System.out.println(" ... Num Persons " + numPersons);
            System.out.println(" ... Start Year " + startYear);
            System.out.println(" ... Num Years " + numYears);
        } catch (Exception e) {
            System.out.println("Error reading scale factors or conf");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static Integer intConf(GeneratorConfiguration conf, ParameterNames param) {
        return Integer.parseInt(conf.get(param.toString()));
    }

    private static Long longConf(GeneratorConfiguration conf, ParameterNames param) {
        return Long.parseLong(conf.get(param.toString()));
    }

    private static Double doubleConf(GeneratorConfiguration conf, ParameterNames param) {
        return Double.parseDouble(conf.get(param.toString()));
    }

    private static String stringConf(GeneratorConfiguration conf, ParameterNames param) {
        return conf.get(param.toString());
    }

    private static double scale(long numPersons, double mean) {
        return Math.log10(mean * numPersons / 2 + numPersons);
    }

    public static DegreeDistribution getDegreeDistribution() {
        DegreeDistribution output;
        switch (degreeDistribution) {
            case "Facebook":
                output = new FacebookDegreeDistribution();
                break;
            case "Zipf":
                output = new ZipfDistribution();
                break;
            default:
                throw new IllegalStateException("Unexpected degree distribution: "
                        + degreeDistribution);
        }

        return output;
    }

}
