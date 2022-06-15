package transaction;

import common.Database;

import java.io.IOException;

/**
 * @author HP
 * @date 2022/5/21
 */
public class Transaction {

    private final TransactionId tid;
    volatile boolean started = false;

    public Transaction() {
        tid = new TransactionId();
    }

    /** Start the transaction running */
    public void start() {
        started = true;
    }

    public TransactionId getId() {
        return tid;
    }

    /** Finish the transaction */
    public void commit() throws IOException {
        transactionComplete(false);
    }

    /** Finish the transaction */
    public void abort() throws IOException {
        transactionComplete(true);
    }

    /** Handle the details of transaction commit / abort */
    public void transactionComplete(boolean abort) throws IOException {

        if (started) {
            // Release locks and flush pages if needed
            Database.getBufferPool().transactionComplete(tid, !abort); // release locks

            //setting this here means we could possibly write multiple abort records -- OK?
            started = false;
        }
    }
}
