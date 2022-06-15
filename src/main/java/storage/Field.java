package storage;

import common.Type;
import execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author HP
 * @date 2022/5/21
 */
public interface Field extends Serializable {

    void serialize(DataOutputStream dos) throws IOException;

    boolean compare(Predicate.Op op, Field value);

    Type getType();

    @Override
    int hashCode();
    @Override
    boolean equals(Object field);

    @Override
    String toString();
}
