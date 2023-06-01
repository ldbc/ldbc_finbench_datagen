package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class Transfer implements DynamicActivity, Serializable {
    private Account fromAccount;
    private Account toAccount;
    private double amount;
    private String ordernum;
    private String comment;
    private String status;
    private String payType;
    private String goodsType;
    private long creationDate;
    private long deletionDate;
    private long multiplicityId;
    private boolean isExplicitlyDeleted;

    public Transfer(Account fromAccount, Account toAccount, double amount, long creationDate, long deletionDate,
                    long multiplicityId, boolean isExplicitlyDeleted) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.multiplicityId = multiplicityId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static Transfer createTransferAndReturn(RandomGeneratorFarm farm, Account from, Account to,
                                                   long multiplicityId,
                                                   double amount) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate =
            Dictionaries.dates.randomAccountToAccountDate(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_DATE), from, to,
                                                          deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        Transfer transfer = new Transfer(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete);

        // Set ordernum
        String ordernum = Dictionaries.numbers.generateOrdernum(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_ORDERNUM));
        transfer.setOrdernum(ordernum);

        // Set comment
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomText(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_COMMENT));
        transfer.setComment(comment);

        // Set payType
        String paytype =
            Dictionaries.transferTypes.getUniformDistRandomText(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_PAYTYPE));
        transfer.setPayType(paytype);

        // Set goodsType
        String goodsType =
            Dictionaries.transferTypes.getUniformDistRandomText(
                farm.get(RandomGeneratorFarm.Aspect.TRANSFER_GOODSTYPE));
        transfer.setGoodsType(goodsType);

        from.getTransferOuts().add(transfer);
        to.getTransferIns().add(transfer);
        return transfer;
    }

    public static Transfer createLoanTransferAndReturn(RandomGeneratorFarm farm, Account from, Account to,
                                                       long multiplicityId,
                                                       double amount) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate =
            Dictionaries.dates.randomAccountToAccountDate(farm.get(RandomGeneratorFarm.Aspect.LOAN_SUBEVENTS_DATE),
                                                          from, to,
                                                          deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        Transfer transfer = new Transfer(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete);

        // Set ordernum
        String ordernum = Dictionaries.numbers.generateOrdernum(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_ORDERNUM));
        transfer.setOrdernum(ordernum);

        // Set comment
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomText(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_COMMENT));
        transfer.setComment(comment);

        // Set payType
        String paytype =
            Dictionaries.transferTypes.getUniformDistRandomText(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_PAYTYPE));
        transfer.setPayType(paytype);

        // Set goodsType
        String goodsType =
            Dictionaries.transferTypes.getUniformDistRandomText(
                farm.get(RandomGeneratorFarm.Aspect.TRANSFER_GOODSTYPE));
        transfer.setGoodsType(goodsType);

        from.getTransferOuts().add(transfer);
        to.getTransferIns().add(transfer);
        return transfer;
    }

    public static class FullComparator implements Comparator<Transfer> {

        public int compare(Transfer a, Transfer b) {
            long res = (a.fromAccount.getAccountId() - b.fromAccount.getAccountId());
            if (res > 0) {
                return 1;
            }
            if (res < 0) {
                return -1;
            }
            long res2 = a.creationDate - b.getCreationDate();
            if (res2 > 0) {
                return 1;
            }
            if (res2 < 0) {
                return -1;
            }
            return 0;
        }

    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public Account getFromAccount() {
        return fromAccount;
    }

    public void setFromAccount(Account fromAccount) {
        this.fromAccount = fromAccount;
    }

    public Account getToAccount() {
        return toAccount;
    }

    public void setToAccount(Account toAccount) {
        this.toAccount = toAccount;
    }

    public long getMultiplicityId() {
        return multiplicityId;
    }

    public void setMultiplicityId(long multiplicityId) {
        this.multiplicityId = multiplicityId;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }


    public String getOrdernum() {
        return ordernum;
    }

    public void setOrdernum(String ordernum) {
        this.ordernum = ordernum;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public String getGoodsType() {
        return goodsType;
    }

    public void setGoodsType(String goodsType) {
        this.goodsType = goodsType;
    }

    @Override
    public String toString() {
        return "Transfer{" + "fromAccount=" + fromAccount + ", toAccount=" + toAccount + ", amount=" + amount
            + ", creationDate=" + creationDate + ", deletionDate=" + deletionDate + ", multiplicityId=" + multiplicityId
            + ", isExplicitlyDeleted=" + isExplicitlyDeleted + '}';
    }
}
