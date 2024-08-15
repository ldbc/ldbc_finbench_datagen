package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.edges.CompanyGuaranteeCompany;
import ldbc.finbench.datagen.entities.edges.CompanyInvestCompany;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
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
    private List<CompanyOwnAccount> companyOwnAccounts;
    private List<CompanyInvestCompany> companyInvestCompanies;
    private HashSet<CompanyGuaranteeCompany> guaranteeSrc;
    private HashSet<CompanyGuaranteeCompany> guaranteeDst;
    private List<CompanyApplyLoan> companyApplyLoans;

    public Company() {
        companyOwnAccounts = new LinkedList<>();
        companyInvestCompanies = new LinkedList<>();
        guaranteeSrc = new HashSet<>();
        guaranteeDst = new HashSet<>();
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
        if (this.equals(to) || guaranteeSrc.contains(to) || guaranteeDst.contains(to)) {
            return false;
        }

        return true;
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

    public List<CompanyInvestCompany> getCompanyInvestCompanies() {
        return companyInvestCompanies;
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
