package storage;

import common.DbException;
import transaction.TransactionAbortedException;

/**
 * @author HP
 * @date 2022/5/21
 */
public interface DbFileIterator {

    void open() throws DbException, TransactionAbortedException;

    boolean hasNext() throws DbException, TransactionAbortedException;

    Tuple next() throws DbException, TransactionAbortedException;

    void rewind() throws DbException, TransactionAbortedException;

    void close();
}
