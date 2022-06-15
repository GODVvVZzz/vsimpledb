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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final TupleDesc tupleDesc;
    private Tuple deleteTuple;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"deleteNums"});
        this.deleteTuple = null;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (deleteTuple != null) {
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int deleteNums = 0;
        while (child.hasNext()) {
            try {
                bufferPool.deleteTuple(tid, child.next());
                deleteNums++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        deleteTuple = new Tuple(tupleDesc);
        deleteTuple.setField(0, new IntField(deleteNums));
        return deleteTuple;
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
