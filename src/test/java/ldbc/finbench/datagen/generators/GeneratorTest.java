package ldbc.finbench.datagen.generators;

import java.util.Map;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.DatagenContext;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.dictionary.Dictionaries;
import ldbc.finbench.datagen.generator.generators.PersonGenerator;
import ldbc.finbench.datagen.util.ConfigParser;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import org.junit.Test;

public class GeneratorTest {

    @Test
    public void testPersonGenerator() {
        Map<String, String> config = ConfigParser.readConfig("src/main/resources/params_default.ini");
        DatagenContext.initialize(new GeneratorConfiguration(config));
        PersonGenerator personGenerator = new PersonGenerator(new GeneratorConfiguration(config),"Facebook");
        Person person = personGenerator.generatePerson();
        assert null != person;
    }

    @Test
    public void testDatagenContext() {
        Map<String, String> config = ConfigParser.readConfig("src/main/resources/params_default.ini");
        DatagenContext.initialize(new GeneratorConfiguration(config));
        System.out.println(DatagenParams.baseProbCorrelated);
        System.out.println(Dictionaries.personNames.getNumNames());
    }

}
