package storage;

import common.Type;
import execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author HP
 * @date 2022/5/21
 */
public class DoubleField implements Field{
    private static final long serialVersionUID = 1L;

    private final double value;

    public DoubleField(double value) {
        this.value = value;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeDouble(value);
    }

    @Override
    public boolean compare(Predicate.Op op, Field val) {
        DoubleField iVal = (DoubleField) val;

        switch (op) {
            case EQUALS:
            case LIKE:
                return value == iVal.value;
            case NOT_EQUALS:
                return value != iVal.value;
            case GREATER_THAN:
                return value > iVal.value;
            case GREATER_THAN_OR_EQ:
                return value >= iVal.value;
            case LESS_THAN:
                return value < iVal.value;
            case LESS_THAN_OR_EQ:
                return value <= iVal.value;
        }

        return false;
    }

    @Override
    public Type getType() {
        return Type.DOUBLE_TYPE;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    @Override
    public int hashCode() {
        return (""+value).hashCode();
    }

    @Override
    public boolean equals(Object field) {
        if (!(field instanceof DoubleField)) {
            return false;
        }
        return ((DoubleField) field).value == value;
    }
}
