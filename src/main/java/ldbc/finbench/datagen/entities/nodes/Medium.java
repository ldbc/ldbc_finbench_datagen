package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.SignIn;

public class Medium implements Serializable {
    private long mediumId;
    private String mediumName;
    private final List<SignIn> signIns;
    private long creationDate;
    private boolean isBlocked;
    private long lastLogin;
    private String riskLevel;

    public Medium() {
        signIns = new ArrayList<>();
    }

    public Medium(long mediumId, String mediumName, long creationDate, boolean isBlocked) {
        signIns = new ArrayList<>();
        this.mediumId = mediumId;
        this.mediumName = mediumName;
        this.creationDate = creationDate;
        this.isBlocked = isBlocked;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Medium) {
            Medium other = (Medium) obj;
            return mediumId == other.mediumId;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(mediumId);
    }

    public long getMediumId() {
        return mediumId;
    }

    public void setMediumId(long mediumId) {
        this.mediumId = mediumId;
    }

    public String getMediumName() {
        return mediumName;
    }

    public void setMediumName(String mediumName) {
        this.mediumName = mediumName;
    }

    public List<SignIn> getSignIns() {
        return signIns;
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

    public long getLastLogin() {
        return lastLogin;
    }

    public void setLastLogin(long lastLogin) {
        this.lastLogin = lastLogin;
    }

    public String getRiskLevel() {
        return riskLevel;
    }

    public void setRiskLevel(String riskLevel) {
        this.riskLevel = riskLevel;
    }
}
