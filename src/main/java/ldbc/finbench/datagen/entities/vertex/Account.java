package ldbc.finbench.datagen.entities.vertex;

public class Account {
    private long accountId;
    private long createTime;
    private boolean isBlocked;
    private String type;


    public Account(long accountId, long createTime, boolean isBlocked, String type) {
        this.accountId = accountId;
        this.createTime = createTime;
        this.isBlocked = isBlocked;
        this.type = type;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
