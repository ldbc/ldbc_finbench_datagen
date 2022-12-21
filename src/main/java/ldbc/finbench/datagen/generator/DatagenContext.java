package ldbc.finbench.datagen.generator;

import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.GeneratorConfiguration;

public class DatagenContext {

    private static volatile boolean initialized = false;

    public static synchronized void initialize(GeneratorConfiguration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            // todo release the Dictionaries
            //  Dictionaries.loadDictionaries();
            initialized = true;
        }
    }
}
