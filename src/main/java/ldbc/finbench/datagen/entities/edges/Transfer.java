/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.Comparator;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class Transfer implements DynamicActivity, Serializable {
    private final long fromAccountId;
    private final long toAccountId;
    private final double amount;
    private final long creationDate;
    private final long deletionDate;
    private final long multiplicityId;
    private final boolean isExplicitlyDeleted;
    private String ordernum;
    private String comment;
    private String payType;
    private String goodsType;

    public Transfer(Account fromAccount, Account toAccount, double amount, long creationDate, long deletionDate,
                    long multiplicityId, boolean isExplicitlyDeleted) {
        this.fromAccountId = fromAccount.getAccountId();
        this.toAccountId = toAccount.getAccountId();
        this.amount = amount;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.multiplicityId = multiplicityId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public static void createTransfer(RandomGeneratorFarm farm, Account from, Account to,
                                          long multiplicityId) {
        long deleteDate = Math.min(from.getDeletionDate(), to.getDeletionDate());
        long creationDate =
            Dictionaries.dates.randomAccountToAccountDate(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_DATE), from, to,
                                                          deleteDate);
        boolean willDelete = from.isExplicitlyDeleted() && to.isExplicitlyDeleted();
        double amount =
            farm.get(RandomGeneratorFarm.Aspect.TRANSFER_AMOUNT).nextDouble() * DatagenParams.transferMaxAmount;
        Transfer transfer = new Transfer(from, to, amount, creationDate, deleteDate, multiplicityId, willDelete);

        // Set ordernum
        String ordernum = Dictionaries.numbers.generateOrdernum(farm.get(RandomGeneratorFarm.Aspect.TRANSFER_ORDERNUM));
        transfer.setOrdernum(ordernum);

        // Set comment
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
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

        // Attention: record the edge both in src and dst to meet degree distribution
        from.getTransferOuts().add(transfer);
        to.getTransferIns().add(transfer);
    }

    public static void createLoanTransfer(RandomGeneratorFarm farm, Account from, Account to,
                                              Loan loan, long multiplicityId,
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
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
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

        //from.getTransferOuts().add(transfer);
        //to.getTransferIns().add(transfer);
        loan.addLoanTransfer(transfer);
    }

    public static class FullComparator implements Comparator<Transfer> {

        public int compare(Transfer a, Transfer b) {
            long res = (a.getFromAccountId() - b.getToAccountId());
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

    public long getFromAccountId() {
        return fromAccountId;
    }

    public long getToAccountId() {
        return toAccountId;
    }

    public long getMultiplicityId() {
        return multiplicityId;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
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
        return "Transfer{" + "fromAccount=" + fromAccountId + ", toAccount=" + toAccountId + ", amount=" + amount
            + ", creationDate=" + creationDate + ", deletionDate=" + deletionDate + ", multiplicityId=" + multiplicityId
            + ", isExplicitlyDeleted=" + isExplicitlyDeleted + '}';
    }
}
