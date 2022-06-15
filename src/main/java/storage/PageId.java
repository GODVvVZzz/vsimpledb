package storage;

/**
 * @author HP
 * @date 2022/5/21
 */
public interface PageId {

    int[] serialize();


    int getTableId();


    @Override
    int hashCode();

    @Override
    boolean equals(Object o);

    int getPageNumber();
}
