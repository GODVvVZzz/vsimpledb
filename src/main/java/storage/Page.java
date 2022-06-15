package storage;

import transaction.TransactionId;

/**
 * @author HP
 * @date 2022/5/21
 */
public interface Page {
    PageId getId();

    TransactionId isDirty();

    void markDirty(boolean dirty, TransactionId tid);

    byte[] getPageData();
}
