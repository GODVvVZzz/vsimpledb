package execution;

import storage.Field;
import storage.Tuple;

import java.io.Serializable;

/**
 * @author HP
 * @date 2022/5/21
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        public static Op getOp(int i) {
            return values()[i];
        }

        @Override
        public String toString() {
            if (this == EQUALS) {
                return "=";
            }
            if (this == GREATER_THAN) {
                return ">";
            }
            if (this == LESS_THAN) {
                return "<";
            }
            if (this == LESS_THAN_OR_EQ) {
                return "<=";
            }
            if (this == GREATER_THAN_OR_EQ) {
                return ">=";
            }
            if (this == LIKE) {
                return "LIKE";
            }
            if (this == NOT_EQUALS) {
                return "<>";
            }
            throw new IllegalStateException("impossible to reach here");
        }
    }

    private final int field;
    private final Op op;
    private final Field operand;
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    public int getField()
    {
        // some code goes here
        return field;
    }

    public Op getOp()
    {
        // some code goes here
        return op;
    }

    public Field getOperand()
    {
        // some code goes here
        return operand;
    }

    public boolean filter(Tuple t) {
        // some code goes here
        if (t == null) {
            return false;
        }
        Field f = t.getField(this.field);
        return f.compare(this.op, this.operand);
    }

    @Override
    public String toString() {
        // some code goes here
        return "f = " + field + " op = " + op.toString() + " operand = " + operand.toString();
    }
}

