package ldbc.finbench.datagen.entities.nodes;

public class Person {
    private long personId;
    private String personName;
    private byte gender;
    private long creationDate;
    private long maxDegree;
    private boolean isBlocked;


    public Person(long personId, String personName, byte gender, long creationDate, long maxDegree, boolean isBlocked) {
        this.personId = personId;
        this.personName = personName;
        this.gender = gender;
        this.creationDate = creationDate;
        this.maxDegree = maxDegree;
        this.isBlocked = isBlocked;
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(long personId) {
        this.personId = personId;
    }

    public String getPersonName() {
        return personName;
    }

    public void setPersonName(String personName) {
        this.personName = personName;
    }

    public byte getGender() {
        return gender;
    }

    public void setGender(byte gender) {
        this.gender = gender;
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
