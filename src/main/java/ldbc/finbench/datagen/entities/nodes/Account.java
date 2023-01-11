package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;

public class Account implements Serializable {
    private long accountId;
    private long createTime;
    private long maxDegree;
    private String type;
    private boolean isBlocked;


    public Account(long accountId, long createTime,long maxDegree, String type,  boolean isBlocked) {
        this.accountId = accountId;
        this.createTime = createTime;
        this.maxDegree = maxDegree;
        this.type = type;
        this.isBlocked = isBlocked;
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

    public long getMaxDegree() {
        return maxDegree;
    }

    public void setMaxDegree(long maxDegree) {
        this.maxDegree = maxDegree;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }
}
