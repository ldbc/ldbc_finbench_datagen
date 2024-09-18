package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.edges.CompanyGuaranteeCompany;
import ldbc.finbench.datagen.entities.edges.CompanyInvestCompany;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;

public class Company implements Serializable {
    private long companyId;
    private String companyName;
    private long creationDate;
    private boolean isBlocked;
    private int countryId;
    private int cityId;
    private String business;
    private String description;
    private String url;
    private final List<CompanyOwnAccount> companyOwnAccounts;
    // invested by persons
    private final List<PersonInvestCompany> personInvestCompanies;
    // invested by companies
    private final List<CompanyInvestCompany> companyInvestCompanies;
    private final LinkedHashSet<CompanyGuaranteeCompany> guaranteeSrc;
    private final LinkedHashSet<CompanyGuaranteeCompany> guaranteeDst;
    private final List<CompanyApplyLoan> companyApplyLoans;

    public Company() {
        companyOwnAccounts = new LinkedList<>();
        personInvestCompanies = new LinkedList<>();
        companyInvestCompanies = new LinkedList<>();
        guaranteeSrc = new LinkedHashSet<>();
        guaranteeDst = new LinkedHashSet<>();
        companyApplyLoans = new LinkedList<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Company) {
            Company company = (Company) obj;
            return this.companyId == company.companyId;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(companyId);
    }

    public boolean canGuarantee(Company to) {
        // can not: equal, guarantee the same company twice, guarantee cyclically
        return !this.equals(to) && !guaranteeSrc.contains(to) && !guaranteeDst.contains(to);
    }

    public long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(long companyId) {
        this.companyId = companyId;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public List<CompanyOwnAccount> getCompanyOwnAccounts() {
        return companyOwnAccounts;
    }

    public List<PersonInvestCompany> getPersonInvestCompanies() {
        return personInvestCompanies;
    }

    public List<CompanyInvestCompany> getCompanyInvestCompanies() {
        return companyInvestCompanies;
    }

    public Company scaleInvestmentRatios() {
        double sum = 0;
        for (PersonInvestCompany pic : personInvestCompanies) {
            sum += pic.getRatio();
        }
        for (CompanyInvestCompany cic : companyInvestCompanies) {
            sum += cic.getRatio();
        }
        for (PersonInvestCompany pic : personInvestCompanies) {
            pic.scaleRatio(sum);
        }
        for (CompanyInvestCompany cic : companyInvestCompanies) {
            cic.scaleRatio(sum);
        }
        return this;
    }

    public boolean hasInvestedBy(Company company) {
        for (CompanyInvestCompany cic : companyInvestCompanies) {
            if (cic.getFromCompanyId() == company.companyId) {
                return true;
            }
        }
        return false;
    }

    public boolean hasInvestedBy(Person person) {
        for (PersonInvestCompany pic : personInvestCompanies) {
            if (pic.getPersonId() == person.getPersonId()) {
                return true;
            }
        }
        return false;
    }

    public HashSet<CompanyGuaranteeCompany> getGuaranteeSrc() {
        return guaranteeSrc;
    }

    public HashSet<CompanyGuaranteeCompany> getGuaranteeDst() {
        return guaranteeDst;
    }

    public List<CompanyApplyLoan> getCompanyApplyLoans() {
        return companyApplyLoans;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }

    public String getCountryName() {
        return Dictionaries.places.getPlaceName(countryId);
    }

    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }

    public String getCityName() {
        return Dictionaries.places.getPlaceName(cityId);
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
