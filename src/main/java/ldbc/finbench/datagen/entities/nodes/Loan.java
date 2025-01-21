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

package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Deposit;
import ldbc.finbench.datagen.entities.edges.Repay;
import ldbc.finbench.datagen.entities.edges.Transfer;

public class Loan implements Serializable, Comparable<Loan> {
    private long loanId;
    private double loanAmount;
    private double balance;
    private long creationDate;
    private long maxDegree;
    private String usage;
    private final double interestRate;
    private PersonOrCompany ownerType;
    private Person ownerPerson;
    private Company ownerCompany;
    private final List<Deposit> deposits;
    private final List<Repay> repays;
    private final List<Transfer> loanTransfers;

    public Loan(long loanId, double loanAmount, double balance, long creationDate, long maxDegree,
                String usage, double interestRate) {
        this.loanId = loanId;
        this.loanAmount = loanAmount;
        this.balance = balance;
        this.creationDate = creationDate;
        this.maxDegree = maxDegree;
        this.usage = usage;
        this.interestRate = interestRate;
        deposits = new LinkedList<>();
        repays = new LinkedList<>();
        loanTransfers = new LinkedList<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Loan) {
            Loan loan = (Loan) obj;
            return loanId == loan.getLoanId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(loanId);
    }

    @Override
    public int compareTo(Loan o) {
        return Long.compare(loanId, o.getLoanId());
    }

    public long getLoanId() {
        return loanId;
    }

    public void setLoanId(long loanId) {
        this.loanId = loanId;
    }

    public double getLoanAmount() {
        return loanAmount;
    }

    public void setLoanAmount(double loanAmount) {
        this.loanAmount = loanAmount;
    }

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public PersonOrCompany getOwnerType() {
        return ownerType;
    }

    public void setOwnerType(PersonOrCompany ownerType) {
        this.ownerType = ownerType;
    }

    public long getMaxDegree() {
        return maxDegree;
    }

    public void setMaxDegree(long maxDegree) {
        this.maxDegree = maxDegree;
    }

    public List<Deposit> getDeposits() {
        return deposits;
    }

    public void addDeposit(Deposit deposit) {
        balance -= deposit.getAmount();
        deposits.add(deposit);
    }

    public void addRepay(Repay repay) {
        balance += repay.getAmount();
        repays.add(repay);
    }

    public List<Repay> getRepays() {
        return repays;
    }

    public void addLoanTransfer(Transfer transfer) {
        loanTransfers.add(transfer);
    }

    public List<Transfer> getLoanTransfers() {
        return loanTransfers;
    }

    public Person getOwnerPerson() {
        return ownerPerson;
    }

    public void setOwnerPerson(Person ownerPerson) {
        this.ownerPerson = ownerPerson;
    }

    public Company getOwnerCompany() {
        return ownerCompany;
    }

    public void setOwnerCompany(Company ownerCompany) {
        this.ownerCompany = ownerCompany;
    }

    public String getUsage() {
        return usage;
    }

    public void setUsage(String usage) {
        this.usage = usage;
    }

    public double getInterestRate() {
        return interestRate;
    }
}
