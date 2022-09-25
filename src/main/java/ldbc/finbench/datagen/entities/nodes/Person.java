package ldbc.finbench.datagen.entities.nodes;

public class Person {
    private long personId;
    private String personName;
    private boolean isBlocked;

    public Person(long personId, String personName, boolean isBlocked) {
        this.personId = personId;
        this.personName = personName;
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

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }
}
