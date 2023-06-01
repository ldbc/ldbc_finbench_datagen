package ldbc.finbench.datagen.generation.dictionary;

import java.time.LocalDateTime;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.generators.DateGenerator;

public class Dictionaries {
    public static PersonNameDictionary personNames = null;
    public static CommonTextDictionary companyNames = null;
    public static CommonTextDictionary mediumNames = null;
    public static CommonTextDictionary accountTypes = null;
    public static CommonTextDictionary businessTypes = null;
    public static CommonTextDictionary randomTexts = null;
    public static CommonTextDictionary transferTypes = null;
    public static CommonTextDictionary goodsTypes = null;
    public static CommonTextDictionary loanUsages = null;
    public static CommonTextDictionary loanOrganizations = null;
    public static CommonTextDictionary urls = null;
    public static EmailDictionary emails = null;
    public static CommonTextDictionary accountNicknames = null;
    public static PercentageTextDictionary accountLevels = null;
    public static CommonTextDictionary riskLevels = null;
    public static PercentageTextDictionary guaranteeRelationships = null;
    public static DateGenerator dates = null;
    public static PlaceDictionary places = null;
    public static NumbersGenerator numbers;

    public static void loadDictionaries() {
        personNames = new PersonNameDictionary(DatagenParams.personSurnameFile, ",");
        companyNames = new CommonTextDictionary(DatagenParams.companyNameFile, ",");
        mediumNames = new CommonTextDictionary(DatagenParams.mediumNameFile, ",");
        accountTypes = new CommonTextDictionary(DatagenParams.accountFile, ",");
        businessTypes = new CommonTextDictionary(DatagenParams.businessTypeFile, ",");
        randomTexts = new CommonTextDictionary(DatagenParams.randomTextFile, "\n");
        transferTypes = new CommonTextDictionary(DatagenParams.transferTypeFile, ",");
        goodsTypes = new CommonTextDictionary(DatagenParams.goodsTypeFile, ",");
        loanUsages = new CommonTextDictionary(DatagenParams.loanUsageFile, ",");
        loanOrganizations = new CommonTextDictionary(DatagenParams.loanOrganizationsFile, ",");
        urls = new CommonTextDictionary(DatagenParams.urlFile, ",");
        emails = new EmailDictionary(DatagenParams.emailFile, " ");
        accountNicknames = new CommonTextDictionary(DatagenParams.accountNicknameFile, ",");
        accountLevels = new PercentageTextDictionary(DatagenParams.accountLevelFile, ",");
        riskLevels = new CommonTextDictionary(DatagenParams.riskLevelFile, ",");
        guaranteeRelationships = new PercentageTextDictionary(DatagenParams.guaranteeRelationshipFile, ",");
        places = new PlaceDictionary();
        numbers = new NumbersGenerator();

        dates = new DateGenerator(
            LocalDateTime.of(DatagenParams.startYear, 1, 1, 0, 0, 0),
            LocalDateTime.of(DatagenParams.startYear + DatagenParams.numYears, 1, 1, 0, 0, 0)
        );
    }

}
