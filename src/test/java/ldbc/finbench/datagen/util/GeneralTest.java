package ldbc.finbench.datagen.util;

import java.util.Map;
import org.junit.Test;

public class GeneralTest {

    @Test
    public void testConfigParser() {
        Map<String, String> config = ConfigParser.readConfig("src/main/resources/parameters/params_default.ini");
        System.out.println(config);
        assert config.size() > 0;
    }

}
