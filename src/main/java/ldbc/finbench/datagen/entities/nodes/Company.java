package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
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
    //    private long maxDegree;
    private boolean isBlocked;
    private int countryId;
    private int cityId;
    private String business;
    private String description;
    private String url;
    private List<Account> accounts;
    private List<Loan> loans;
    private List<CompanyOwnAccount> companyOwnAccounts;
    private List<CompanyInvestCompany> companyInvestCompanies;
    private List<CompanyGuaranteeCompany> guaranteeSrc;
    private List<CompanyGuaranteeCompany> guaranteeDst;
    private List<CompanyApplyLoan> companyApplyLoans;

    public Company() {
        accounts = new ArrayList<>();
        loans = new ArrayList<>();
        companyOwnAccounts = new ArrayList<>();
        companyInvestCompanies = new ArrayList<>();
        guaranteeSrc = new ArrayList<>();
        guaranteeDst = new ArrayList<>();
        companyApplyLoans = new ArrayList<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Company) {
            Company company = (Company) obj;
            return this.companyId == company.companyId;
        }
        return false;
    }

    public boolean canGuarantee(Company to) {
        if (this.getCompanyId() == to.getCompanyId()) {
            return false;
        }
        // can not guarantee the same company twice
        for (CompanyGuaranteeCompany guarantee : guaranteeSrc) {
            if (guarantee.getToCompany().getCompanyId() == to.getCompanyId()) {
                return false;
            }
        }
        // can not guarantee cyclically
        for (CompanyGuaranteeCompany guarantee : guaranteeDst) {
            if (guarantee.getFromCompany().getCompanyId() == to.getCompanyId()) {
                return false;
            }
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

    public void setCompanyOwnAccounts(List<CompanyOwnAccount> companyOwnAccounts) {
        this.companyOwnAccounts = companyOwnAccounts;
    }

    public List<CompanyInvestCompany> getCompanyInvestCompanies() {
        return companyInvestCompanies;
    }

    public void setCompanyInvestCompanies(List<CompanyInvestCompany> companyInvestCompanies) {
        this.companyInvestCompanies = companyInvestCompanies;
    }

    public List<CompanyGuaranteeCompany> getGuaranteeSrc() {
        return guaranteeSrc;
    }

    public void setGuaranteeSrc(List<CompanyGuaranteeCompany> guaranteeSrc) {
        this.guaranteeSrc = guaranteeSrc;
    }

    public List<CompanyGuaranteeCompany> getGuaranteeDst() {
        return guaranteeDst;
    }

    public void setGuaranteeDst(List<CompanyGuaranteeCompany> guaranteeDst) {
        this.guaranteeDst = guaranteeDst;
    }

    public List<CompanyApplyLoan> getCompanyApplyLoans() {
        return companyApplyLoans;
    }

    public void setCompanyApplyLoans(List<CompanyApplyLoan> companyApplyLoans) {
        this.companyApplyLoans = companyApplyLoans;
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

    public List<Account> getAccounts() {
        return accounts;
    }

    public void setAccounts(List<Account> accounts) {
        this.accounts = accounts;
    }

    public List<Loan> getLoans() {
        return loans;
    }

    public void setLoans(List<Loan> loans) {
        this.loans = loans;
    }

    public int getCountryId() {
        return countryId;
    }

    public String getCountryName() {
        return Dictionaries.places.getPlaceName(countryId);
    }

    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }

    public int getCityId() {
        return cityId;
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
