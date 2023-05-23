package ldbc.finbench.datagen.util;

import java.util.Map;
import ldbc.finbench.datagen.config.ConfigParser;
import org.junit.Test;

public class GeneralTest {

    @Test
    public void testConfigParser() {
        Map<String, String> config = ConfigParser.readConfig("src/main/resources/params_default.ini");
        System.out.println(config);
        assert config.size() > 0;
    }

}
