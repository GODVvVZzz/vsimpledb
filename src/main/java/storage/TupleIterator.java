package storage;

import execution.OpIterator;

import java.util.Iterator;

/**
 * @author HP
 * @date 2022/5/21
 */
public class TupleIterator  implements OpIterator {

    private static final long serialVersionUID = 1L;
    Iterator<Tuple> i = null;
    TupleDesc td = null;
    Iterable<Tuple> tuples = null;

    public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
        this.td = td;
        this.tuples = tuples;

        for (Tuple t : tuples) {
            if (!t.getTupleDesc().equals(td)) {
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
            }
        }
    }

    @Override
    public void open() {
        i = tuples.iterator();
    }

    @Override
    public boolean hasNext() {
        return i.hasNext();
    }

    @Override
    public Tuple next() {
        return i.next();
    }

    @Override
    public void rewind() {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void close() {
        i = null;
    }
}
