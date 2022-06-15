package storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author HP
 * @date 2022/5/21
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pageId;
    private final int tupleNumber;

    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pageId = pid;
        this.tupleNumber = tupleno;
    }

    public int getTupleNumber() {
        // some code goes here
        return this.tupleNumber;
    }

    public PageId getPageId() {
        // some code goes here
        return this.pageId;
    }

    @Override
    public boolean equals(Object o) {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        if (o instanceof RecordId) {
            RecordId recordId = (RecordId) o;
            if (recordId.getPageId().equals(this.pageId) && recordId.getTupleNumber() == this.tupleNumber) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return Objects.hash(this.pageId, this.tupleNumber);
    }
}
