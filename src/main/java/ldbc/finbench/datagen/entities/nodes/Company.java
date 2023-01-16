package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.CompanyApplyLoan;
import ldbc.finbench.datagen.entities.edges.CompanyGuaranteeCompany;
import ldbc.finbench.datagen.entities.edges.CompanyInvestCompany;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;

public class Company implements Serializable {
    private long companyId;
    private String companyName;
    private List<CompanyOwnAccount> companyOwnAccounts;
    private List<CompanyInvestCompany> companyInvestCompanies;
    private List<CompanyGuaranteeCompany> companyGuaranteeCompanies;
    private List<CompanyApplyLoan> companyApplyLoans;
    private long creationDate;
    private long maxDegree;
    private boolean isBlocked;

    public Company(long companyId, String companyName, long creationDate, long maxDegree, boolean isBlocked) {
        this.companyId = companyId;
        this.companyName = companyName;
        companyOwnAccounts = new ArrayList<>();
        companyInvestCompanies = new ArrayList<>();
        companyGuaranteeCompanies = new ArrayList<>();
        companyApplyLoans = new ArrayList<>();
        this.creationDate =  creationDate;
        this.maxDegree = maxDegree;
        this.isBlocked = isBlocked;
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

    public List<CompanyGuaranteeCompany> getCompanyGuaranteeCompanies() {
        return companyGuaranteeCompanies;
    }

    public void setCompanyGuaranteeCompanies(List<CompanyGuaranteeCompany> companyGuaranteeCompanies) {
        this.companyGuaranteeCompanies = companyGuaranteeCompanies;
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

    public long getMaxDegree() {
        return maxDegree;
    }

    public void setMaxDegree(long maxDegree) {
        this.maxDegree = maxDegree;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }
}
