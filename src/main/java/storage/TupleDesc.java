package storage;

import common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author HP
 * @date 2022/5/21
 */
public class TupleDesc implements Serializable {

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;


        public final Type fieldType;


        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private List<TDItem> items;

    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    public TupleDesc() {}

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        items = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            items.add(item);
        }
    }

    public TupleDesc(Type[] typeAr) {
        // some code goes here
        items = new ArrayList<>();
        for (Type type : typeAr) {
            TDItem item = new TDItem(type, null);
            items.add(item);
        }
    }

    public List<TDItem> getItems() {
        return items;
    }

    public void setItems(List<TDItem> items) {
        this.items = items;
    }

    public int numFields() {
        // some code goes here
        return items.size();
    }

    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("position " + i + " is not a valid index");
        }
        return items.get(i).fieldName;
    }

    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("position " + i + " is not a valid index");
        }
        return items.get(i).fieldType;
    }

    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("fieldName " + name + " is not founded");
        }
        for (int i = 0; i < items.size(); i++) {
            if (name.equals(items.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException("fieldName " + name + " is not founded");
    }

    public int getSize() {
        // some code goes here
        int bytes = 0;
        for (TDItem item : items) {
            bytes += item.fieldType.getLen();
        }
        return bytes;
    }

    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> items = new ArrayList<>();
        items.addAll(td1.getItems());
        items.addAll(td2.getItems());
        TupleDesc res = new TupleDesc();
        res.setItems(items);
        return res;
    }

    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (!this.getClass().isInstance(o)) {
            return false;
        }
        TupleDesc desc = (TupleDesc) o;
        int len = this.items.size();
        List<TDItem> tdItems = desc.getItems();
        int size = tdItems.size();
        if (len != size) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            TDItem item = this.items.get(i);
            TDItem tdItem = tdItems.get(i);
            if (!item.fieldType.equals(tdItem.fieldType)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            TDItem item = items.get(i);
            builder.append(item.fieldType + "(" + item.fieldName + ")");
            if (i != items.size() - 1) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }
}
