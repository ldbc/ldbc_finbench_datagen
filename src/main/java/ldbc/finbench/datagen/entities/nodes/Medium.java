package ldbc.finbench.datagen.entities.nodes;

public class Medium {
    private long mediumId;
    private String mediumName;
    private long creationDate;
    private long maxDegree;
    private boolean isBlocked;

    public Medium(long mediumId, String mediumName, long creationDate, long maxDegree, boolean isBlocked) {
        this.mediumId = mediumId;
        this.mediumName = mediumName;
        this.creationDate = creationDate;
        this.maxDegree = maxDegree;
        this.isBlocked = isBlocked;
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
