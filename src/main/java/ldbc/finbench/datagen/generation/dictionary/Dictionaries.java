package ldbc.finbench.datagen.generation.dictionary;

import java.time.LocalDateTime;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.generators.DateGenerator;

public class Dictionaries {
    public static CommonTextDictionary personNames = null;
    public static CommonTextDictionary companyNames = null;
    public static CommonTextDictionary mediumNames = null;
    public static CommonTextDictionary accountTypes = null;
    public static CommonTextDictionary businessTypes = null;
    public static CommonTextDictionary businessDescription = null;
    public static CommonTextDictionary loanUsages = null;
    public static CommonTextDictionary urls = null;
    public static EmailDictionary emails = null;
    public static CommonTextDictionary accountNicknames = null;
    public static CommonTextDictionary accountLevels = null;
    public static CommonTextDictionary riskLevels = null;
    public static DateGenerator dates = null;
    public static PlaceDictionary places = null;
    public static NumbersGenerator numbers;

    public static void loadDictionaries() {
        personNames = new CommonTextDictionary(DatagenParams.personSurnameFile, ",");
        companyNames = new CommonTextDictionary(DatagenParams.companyNameFile, ",");
        mediumNames = new CommonTextDictionary(DatagenParams.mediumNameFile, ",");
        accountTypes = new CommonTextDictionary(DatagenParams.accountFile, ",");
        businessTypes = new CommonTextDictionary(DatagenParams.businessTypeFile, ",");
        businessDescription = new CommonTextDictionary(DatagenParams.businessDescriptionsFile, ",");
        loanUsages = new CommonTextDictionary(DatagenParams.loanUsageFile, ",");
        urls = new CommonTextDictionary(DatagenParams.urlFile, ",");
        emails = new EmailDictionary(DatagenParams.emailFile, " ");
        accountNicknames = new CommonTextDictionary(DatagenParams.accountNicknameFile, ",");
        accountLevels = new CommonTextDictionary(DatagenParams.accountLevelFile, ",");
        riskLevels = new CommonTextDictionary(DatagenParams.riskLevelFile, ",");
        places = new PlaceDictionary();
        numbers = new NumbersGenerator();

        dates = new DateGenerator(
            LocalDateTime.of(DatagenParams.startYear, 1, 1, 0, 0, 0),
            LocalDateTime.of(DatagenParams.startYear + DatagenParams.numYears, 1, 1, 0, 0, 0)
        );
    }

}
