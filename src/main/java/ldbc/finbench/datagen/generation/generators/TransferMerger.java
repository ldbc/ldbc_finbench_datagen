package ldbc.finbench.datagen.generation.generators;

import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;

public class TransferMerger {
    // Will be used in implementation of Account-centric transfers generation.
    public Account merge(Iterable<Account> accounts) {
        List<Transfer> transfers = new ArrayList<>();
        Account account = null;
        int index = 0;
        for (Account acc : accounts) {
            if (index == 0) {
                account = new Account(); // TODO: use copy constructor
            }
            transfers.addAll(acc.getTransferOuts());
            index++;
        }
        account.getTransferOuts().clear();
        transfers.sort(new Transfer.FullComparator());
        if (transfers.size() > 0) {
            account.getTransferOuts().addAll(transfers);
        }
        return account;
    }

}
