package common;

import index.BTreeFile;
import storage.DbFile;
import storage.TupleDesc;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author HP
 * @date 2022/5/21
 */
public class Catalog {


    public class Table {
        private DbFile file;
        private String name;
        private String pkeyField;

        public Table(DbFile file, String name, String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }
    }

    private Map<Integer, Table> tables;
    private Map<String, Integer> nameToId;

    public Catalog() {
        tables = new ConcurrentHashMap<>();
        nameToId = new ConcurrentHashMap<>();
    }

    public void addTable(DbFile file, String name, String pkeyField) {
        Table table = new Table(file, name, pkeyField);
        tables.put(file.getId(), table);
        nameToId.put(name, file.getId());
    }


    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if (tables.containsKey(tableid)) {
            Table table = tables.get(tableid);
            return table.file.getTupleDesc();
        }
        throw new NoSuchElementException("Table " + tableid + " does not exist");
    }

    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        if (tables.containsKey(tableid)) {
            return tables.get(tableid).file;
        }
        throw new NoSuchElementException("Table " + tableid + " does not exist");
    }


    public Iterator<String> tableIdIterator() {

        return nameToId.keySet().iterator();
    }

    public String getTableName(int id) {

        if (tables.containsKey(id)) {
            Table table = tables.get(id);
            return table.name;
        }
        return null;
    }

    public void clear() {

        tables.clear();
        nameToId.clear();
    }

    /**
     * 从数据库catalog中读取当前数据库有哪些表
     * 每个表结构是确定的(double,double,double,double)
     * 1.读取catalog.txt
     * 2.加载到数据库catalog
     * @param catalogFile
     */
    public void loadSchema(String catalogFile){
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            while ((line = br.readLine()) != null){
                Type[] types = new Type[]{Type.DOUBLE_TYPE,Type.DOUBLE_TYPE,Type.DOUBLE_TYPE,Type.DOUBLE_TYPE};
                String[] names = new String[]{"x","y","z","w"};
                TupleDesc t = new TupleDesc(types,names);

                File file = new File(Utility.getNowPath() + "\\" + line + ".dat");
                if (!file.exists()){
                    System.out.println("Table: " + line + " doesn't exits.");
                }
                //默认以第一个值作为索引
                BTreeFile bTreeFile = new BTreeFile(file,0,t);
                addTable(bTreeFile,line,"f1");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
