package ldbc.finbench.datagen.generators;

import java.util.Map;
import ldbc.finbench.datagen.config.ConfigParser;
import ldbc.finbench.datagen.config.DatagenConfiguration;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenContext;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.PersonGenerator;
import org.junit.Test;

public class GeneratorTest {
    Map<String, String> config;

    public GeneratorTest() {
        config = ConfigParser.readConfig("src/main/resources/params_default.ini");
        config.putAll(ConfigParser.scaleFactorConf("","0.1")); // use scale factor 0.1
        DatagenContext.initialize(new DatagenConfiguration(config));
    }

    @Test
    public void testPersonGenerator() {
        PersonGenerator personGenerator = new PersonGenerator();
        Person person = personGenerator.generatePerson();
        assert null != person;
    }

    @Test
    public void testDatagenContext() {
        System.out.println(Dictionaries.personNames.getNumNames());
    }

}
