package execution;

import common.DbException;
import storage.Tuple;
import storage.TupleDesc;
import transaction.TransactionAbortedException;

import java.io.Serializable;

/**
 * @author HP
 * @date 2022/5/21
 */
public interface OpIterator extends Serializable {

    void open() throws DbException, TransactionAbortedException;

    boolean hasNext() throws DbException, TransactionAbortedException;

    Tuple next() throws DbException, TransactionAbortedException;

    void rewind() throws DbException, TransactionAbortedException;

    TupleDesc getTupleDesc();

    void close();
}
