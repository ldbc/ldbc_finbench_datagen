package ldbc.finbench.datagen.util;

import java.util.Iterator;
import java.util.Map;

public class GeneratorConfiguration implements Iterable<Map.Entry<String,String>> {
    public final Map<String, String> map;

    public GeneratorConfiguration(Map<String, String> map) {
        this.map = map;
    }

    public String get(String key) {
        return map.get(key);
    }

    // todo get config values

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return this.map.entrySet().iterator();
    }
}
