package ldbc.finbench.datagen.generation;

import ldbc.finbench.datagen.config.DatagenConfiguration;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class DatagenContext {

    private static volatile boolean initialized = false;

    public static synchronized void initialize(DatagenConfiguration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            Dictionaries.loadDictionaries();
            initialized = true;
        }
    }
}
