package ldbc.finbench.datagen.generation;

import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.config.GeneratorConfiguration;

public class DatagenContext {

    private static volatile boolean initialized = false;

    public static synchronized void initialize(GeneratorConfiguration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            Dictionaries.loadDictionaries();
            initialized = true;
        }
    }
}
