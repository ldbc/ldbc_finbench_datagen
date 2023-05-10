package ldbc.finbench.datagen.generator.dictionary;

import java.time.LocalDateTime;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.generators.DateGenerator;

public class Dictionaries {
    public static PersonNameDictionary personNames = null;
    public static CompanyNameDictionary companyNames = null;
    public static MediumNameDictionary mediumNames = null;
    public static AccountDictionary accountTypes = null;
    public static DateGenerator dates = null;

    public static void loadDictionaries() {
        personNames = new PersonNameDictionary();
        companyNames = new CompanyNameDictionary();
        mediumNames = new MediumNameDictionary();
        accountTypes = new AccountDictionary();

        dates = new DateGenerator(
            LocalDateTime.of(DatagenParams.startYear, 1, 1, 0, 0, 0),
            LocalDateTime.of(DatagenParams.startYear + DatagenParams.numYears, 1, 1, 0, 0, 0)
        );
    }

}
