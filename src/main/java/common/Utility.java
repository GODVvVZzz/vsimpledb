package common;

import execution.Predicate;
import storage.TupleDesc;

/**
 * @author HP
 * @date 2022/5/20
 */
public class Utility {

    //所有数据库的根路径
    private static final String path = ".\\databases";

    //当前数据库的路径
    private static String nowPath = "";

    public static String getPath(){
        return path;
    }

    public static void setNowPath(String dbName){
        nowPath = path + "\\" + dbName;
    }

    public static void resetNowPath(){
        nowPath = "";
    }

    public static String getNowPath(){
        return nowPath;
    }


    public static Type[] getTypes(int len) {
        Type[] types = new Type[len];
        for (int i = 0; i < len; ++i) {
            types[i] = Type.INT_TYPE;
        }
        return types;
    }

    /**
     * @return a String array of length len populated with the (possibly null) strings in val,
     * and an appended increasing integer at the end (val1, val2, etc.).
     */
    public static String[] getStrings(int len, String val) {
        String[] strings = new String[len];
        for (int i = 0; i < len; ++i) {
            strings[i] = val + i;
        }
        return strings;
    }

    public static TupleDesc getTupleDesc(int n, String name) {
        if (name.equals("double")){
            Type[] types = new Type[]{Type.DOUBLE_TYPE,Type.DOUBLE_TYPE,Type.DOUBLE_TYPE,Type.DOUBLE_TYPE};
            String[] names = new String[]{"x","y","z","w"};
            TupleDesc t = new TupleDesc(types,names);
            return t;
        }
        return new TupleDesc(getTypes(n), getStrings(n, name));
    }

    public static TupleDesc getTupleDesc(int n) {
        return new TupleDesc(getTypes(n));
    }

    public static Predicate.Op stringToOp(String op){
        switch (op){
            case "=":
                return Predicate.Op.EQUALS;
            case ">":
                return Predicate.Op.GREATER_THAN;
            case "<":
                return Predicate.Op.LESS_THAN;
            case ">=":
                return Predicate.Op.GREATER_THAN_OR_EQ;
            case "<=":
                return Predicate.Op.LESS_THAN_OR_EQ;
            default:
                break;
        }
        return null;
    }
}
