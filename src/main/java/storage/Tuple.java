package storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author HP
 * @date 2022/5/21
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;

    private final Field[] fields;

    private RecordId recordId;

    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.fields = new Field[td.numFields()];
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void setTupleDesc(TupleDesc tupleDesc) {
        this.tupleDesc = tupleDesc;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }

    public void setField(int i, Field f) {
        // some code goes here
        fields[i] = f;
    }

    public Field getField(int i) {
        // some code goes here
        if (fields == null) {
            return null;
        }
        return fields[i];
    }

    @Override
    public String toString() {
        // some code goes here
        // throw new UnsupportedOperationException("Implement this");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < this.fields.length; i++) {
            builder.append(this.fields[i].toString() + " ");
            if (i == this.fields.length - 1) {
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    public Iterator<Field> fields()
    {
        // some code goes here
        return Arrays.asList(this.fields).iterator();
    }
}
