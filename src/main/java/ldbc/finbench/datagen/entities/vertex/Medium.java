package ldbc.finbench.datagen.entities.vertex;

public class Medium {
    private long mediumId;
    private String mediumName;
    private boolean isBlocked;

    public Medium(long mediumId, String mediumName, boolean isBlocked) {
        this.mediumId = mediumId;
        this.mediumName = mediumName;
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

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }
}
