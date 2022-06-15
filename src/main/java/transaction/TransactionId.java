package transaction;

import common.Database;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author HP
 * @date 2022/5/21
 */
public class TransactionId {

    private static final long serialVersionUID = 1L;

    static final AtomicLong counter = new AtomicLong(0);
    final long myid;

    public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TransactionId other = (TransactionId) obj;
        return myid == other.myid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (myid ^ (myid >>> 32));
        return result;
    }
}
