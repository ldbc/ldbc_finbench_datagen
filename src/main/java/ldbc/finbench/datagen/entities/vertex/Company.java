package ldbc.finbench.datagen.entities.vertex;

public class Company {
    private long companyId;
    private String companyName;
    private boolean isBlocked;

    public Company(long companyId, String companyName, boolean isBlocked) {
        this.companyId = companyId;
        this.companyName = companyName;
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

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }
}
