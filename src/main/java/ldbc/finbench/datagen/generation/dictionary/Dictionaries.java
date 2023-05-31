package ldbc.finbench.datagen.generation.dictionary;

import java.time.LocalDateTime;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.generators.DateGenerator;

public class Dictionaries {
    public static PersonNameDictionary personNames = null;
    public static CompanyNameDictionary companyNames = null;
    public static MediumNameDictionary mediumNames = null;
    public static AccountTypeDictionary accountTypes = null;
    public static BusinessTypeDictionary businessTypes = null;
    public static LoanUsageDictionary loanUsages = null;
    public static DateGenerator dates = null;
    public static PlaceDictionary places = null;

    public static void loadDictionaries() {
        personNames = new PersonNameDictionary();
        companyNames = new CompanyNameDictionary();
        mediumNames = new MediumNameDictionary();
        accountTypes = new AccountTypeDictionary();
        businessTypes = new BusinessTypeDictionary();
        loanUsages = new LoanUsageDictionary();

        dates = new DateGenerator(
            LocalDateTime.of(DatagenParams.startYear, 1, 1, 0, 0, 0),
            LocalDateTime.of(DatagenParams.startYear + DatagenParams.numYears, 1, 1, 0, 0, 0)
        );
        places = new PlaceDictionary();
    }

}
