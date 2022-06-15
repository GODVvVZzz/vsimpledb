package parser.command;

import common.Database;
import common.Utility;
import index.BTreeFile;
import storage.TupleDesc;

import java.io.*;
import java.util.Iterator;

/**
 * @author HP
 * @date 2022/5/20
 * create database name;
 * create table name;
 */
public class Create extends Command{

    public Create(String cmd) {
        super(cmd);
    }

    @Override
    public void analysis() {
        if (cmdWords.length != 3){
            System.out.println("语法错误！");
            return;
        }

        if (cmd.contains(" database ")){
            createDatabase();
        }else if (cmd.contains(" table ")){
            createTable();
        }else {
            System.out.println("语法错误！");
        }
    }

    /**
     * 创建数据库
     * 1.一个文件夹
     * 2.Catalog.txt
     * 3.用dbCatalog.txt记录
     */
    public void createDatabase() {

        String txtFilePath = Utility.getPath() + "\\"+ "dbCatalog.txt";
        File file = new File(txtFilePath);
        if (!file.exists()) {
            System.out.println("dbCatalog未创建！");
            return;
        }

        File database = new File(Utility.getPath() + "\\" + cmdWords[2]);
        if(!database.exists()){
            database.mkdir();
            System.out.println("数据库:"+ cmdWords[2] + " 创建成功！");
        }
        else{
            System.out.println("数据库:" + cmdWords[2] +" 已存在！");
            return;
        }

        Utility.setNowPath(cmdWords[2]);
        File catalog = new File(Utility.getNowPath(),"catalog.txt");
        if (!catalog.exists()) {
            try {
                catalog.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            FileOutputStream fos = new FileOutputStream(file,true);
            OutputStreamWriter writer = new OutputStreamWriter(fos);
            writer.write(cmdWords[2] + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("写入dbCatalog失败！");
        }
    }

    public void createTable() {

        if (Utility.getNowPath().equals("") || Utility.getNowPath() == null){
            System.out.println("请先选择数据库！");
            return;
        }

        Iterator<String> tables = Database.getCatalog().tableIdIterator();
        while (tables.hasNext()){
            if (tables.next().equals(cmdWords[2])){
                System.out.println("Table: " + cmdWords[2] + " exits.");
                return;
            }
        }
        File file = new File(Utility.getNowPath() + "\\" + cmdWords[2] + ".dat");
        try {
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(new byte[0]);
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TupleDesc t =Utility.getTupleDesc(4,"double");
        BTreeFile bTreeFile = new BTreeFile(file,0,t);
        Database.getCatalog().addTable(bTreeFile,cmdWords[2],"x");

        try {
            File catalog = new File(Utility.getNowPath() + "\\" + "catalog.txt");
            FileOutputStream fos = new FileOutputStream(catalog,true);
            OutputStreamWriter writer = new OutputStreamWriter(fos);
            writer.write(cmdWords[2] + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Table: " + cmdWords[2] + " 创建成功！");
    }


}
