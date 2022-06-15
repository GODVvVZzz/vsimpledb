package storage;

import common.DbException;
import transaction.TransactionAbortedException;
import transaction.TransactionId;

import java.io.IOException;
import java.util.List;

/**
 * @author HP
 * @date 2022/5/21
 */
public interface DbFile {

    Page readPage(PageId id);

    void writePage(Page p) throws IOException;

    List<Page> insertTuple(TransactionId tid, Tuple t)
            throws IOException, DbException, TransactionAbortedException;

    List<Page> deleteTuple(TransactionId tid, Tuple t)
            throws IOException, DbException, TransactionAbortedException;

    DbFileIterator iterator(TransactionId tid);

    int getId();

    TupleDesc getTupleDesc();
}
