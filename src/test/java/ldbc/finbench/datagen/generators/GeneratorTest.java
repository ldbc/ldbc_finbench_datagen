package ldbc.finbench.datagen.generators;

import static net.andreinc.mockneat.types.enums.DomainSuffixType.POPULAR;
import static net.andreinc.mockneat.types.enums.HostNameType.ADVERB_VERB;
import static net.andreinc.mockneat.types.enums.MarkovChainType.KAFKA;
import static net.andreinc.mockneat.types.enums.MarkovChainType.LOREM_IPSUM;
import static net.andreinc.mockneat.types.enums.URLSchemeType.HTTP;
import static net.andreinc.mockneat.unit.networking.URLs.urls;
import static net.andreinc.mockneat.unit.text.Markovs.markovs;

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
        config.putAll(ConfigParser.scaleFactorConf("", "0.1")); // use scale factor 0.1
        DatagenContext.initialize(new DatagenConfiguration(config));
    }

    @Test
    public void testPersonGenerator() {
        PersonGenerator personGenerator = new PersonGenerator();
        Person person = personGenerator.generatePerson();
        assert null != person;
    }

    @Test
    public void testMockneat() {
        for (int i = 0; i < 10; i++) {
            System.out.println(markovs().size(200).type(KAFKA).get());
        }
        for (int i = 0; i < 10; i++) {
            System.out.println(
                urls().scheme(HTTP).domain(POPULAR).host(ADVERB_VERB).get());
        }
    }

    @Test
    public void testDatagenContext() {
        System.out.println(Dictionaries.personNames.getNumNames());
    }

}
