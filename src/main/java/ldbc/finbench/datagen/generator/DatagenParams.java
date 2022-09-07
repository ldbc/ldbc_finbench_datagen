package ldbc.finbench.datagen.generator;

import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.GeneratorConfiguration;

public class DatagenParams {
    public static final String DICTIONARY_DIRECTORY = "./resources/dictionaries/";

    //TODO add more config constant

    public static final String personNameFile = DICTIONARY_DIRECTORY + "personName.txt";

    private enum ParameterNames{
        BLOCK_SIZE("generator.blockSize");

        private final String name;

        ParameterNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    public static void readConf(GeneratorConfiguration conf, ParameterNames param){
        try{
            ParameterNames[] values = ParameterNames.values();
            for (ParameterNames value : values){
                if (conf.get(value.toString()) == null) {
                    throw new IllegalStateException("Missing " + value.toString() + " parameter");
                }
            }

            //TODO assign value to constant

        }catch (Exception e) {
            System.out.println("Error reading scale factors or conf");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static Integer intConf(GeneratorConfiguration conf, ParameterNames param) {
        return Integer.parseInt(conf.get(param.toString()));
    }

    private static Double doubleConf(GeneratorConfiguration conf, ParameterNames param) {
        return Double.parseDouble(conf.get(param.toString()));
    }

    private static double scale(long numPersons, double mean) {
        return Math.log10(mean * numPersons / 2 + numPersons);
    }

    public static DegreeDistribution getDegreeDistribution() {
        //TODO

        return null;
    }

}
