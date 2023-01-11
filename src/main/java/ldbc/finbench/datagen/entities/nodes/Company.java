package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;

public class Company implements Serializable {
    private long companyId;
    private String companyName;
    private long creationDate;
    private long maxDegree;
    private boolean isBlocked;

    public Company(long companyId, String companyName, long creationDate, long maxDegree, boolean isBlocked) {
        this.companyId = companyId;
        this.companyName = companyName;
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
