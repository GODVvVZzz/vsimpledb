package execution;

import common.Database;
import common.DbException;
import common.Type;
import storage.BufferPool;
import storage.IntField;
import storage.Tuple;
import storage.TupleDesc;
import transaction.TransactionAbortedException;
import transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private Tuple insertTuple;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"insertNums"});
        this.insertTuple = null;
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    @Override
    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (insertTuple != null) {
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int insertTuples = 0;
        while (child.hasNext()) {
            try {
                bufferPool.insertTuple(tid, tableId, child.next());
                insertTuples++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        insertTuple = new Tuple(this.tupleDesc);
        insertTuple.setField(0, new IntField(insertTuples));
        return insertTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
