package parser.command;

import common.Database;
import common.Utility;

import java.io.*;
import java.util.ArrayList;


/**
 * @author HP
 * @date 2022/5/20
 * drop database name;
 * drop table name;
 * 1.删除数据库
 * 删除相应的文件夹和dbcatalog.txt里面的记录
 * 2.删除表
 * 删除相应的数据文件和catalog.txt里面的记录
 *
 * 如果删除的是当前选择的数据库  要重新设置nowPath和清空catalog
 * 通过nowpath来判断删除的是不是当前选择的文件夹
 *
 * 先把txt文件全部读出来，删除相应的记录 再写回去
 */
public class Drop extends Command{

    public Drop(String cmd) {
        super(cmd);
    }

    @Override
    public void analysis(){
        if (cmdWords.length != 3){
            System.out.println("语法错误！");
            return;
        }

        if (cmd.contains(" database ")){
            dropDatabase();
        }else if (cmd.contains(" table ")){
            if (!dbAndTableCheck.check(cmdWords[2])){
                return;
            }
            dropTable();
        }else {
            System.out.println("语法错误！");
        }
    }

    private void dropTable() {

        String deleteTable = Utility.getNowPath() + "\\" + cmdWords[2] + ".dat";
        String catalogPath = Utility.getNowPath() + "\\" + "catalog.txt";

        ArrayList<String> tableNames = readDataFromTxt(catalogPath);

        boolean contains = tableNames.contains(cmdWords[2]);
        if (contains){
            tableNames.remove(cmdWords[2]);
        }else {
            System.out.println("此表在当前数据库中不存在！");
            return;
        }

        writeDataToTxt(catalogPath,tableNames);

        File file = new File(deleteTable);
        file.delete();

        Database.getCatalog().clear();
        Database.getCatalog().loadSchema(catalogPath);

        System.out.println("Table: " + cmdWords[2] +" 删除成功！");
    }

    private void dropDatabase() {

        String deleteDB = Utility.getPath() + "\\" + cmdWords[2];
        String dbCatalogPath = Utility.getPath() + "\\" + "dbCatalog.txt";

        ArrayList<String> dbNames = readDataFromTxt(dbCatalogPath);

        boolean contains = dbNames.contains(cmdWords[2]);
        if (contains){
            dbNames.remove(cmdWords[2]);
        }else {
            System.out.println("此数据库不存在！");
            return;
        }

        writeDataToTxt(dbCatalogPath,dbNames);

        File file = new File(deleteDB);
        deleteFile(file);

        //如果删除的是当前选择的数据库,清空内存中的bufferPool和catalog
        if (Utility.getNowPath().equals(deleteDB)){
            Utility.resetNowPath();
            Database.reset();
        }

        System.out.println("database: " + cmdWords[2] +" 删除成功！");
    }

    /**
     * 将txt里面的数据按行读出来放到集合中
     * @param filePath
     * @return
     */
    private ArrayList<String> readDataFromTxt(String filePath){

        ArrayList<String> results = new ArrayList<>();
        try {
            String line = "";
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            while ((line = br.readLine()) != null){
                results.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    /**
     * 清空txt，再往txt里写数据
     * @param filePath
     */
    private void writeDataToTxt(String filePath, ArrayList<String> dbNames){
        File dbCatalog = new File(filePath);
        try {
            FileOutputStream fos = new FileOutputStream(dbCatalog);
            OutputStreamWriter writer = new OutputStreamWriter(fos);
            for (String dbName : dbNames) {
                writer.write(dbName + "\n");
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 先根遍历序递归删除文件夹
     *
     * @param dirFile 要被删除的文件或者目录
     * @return 删除成功返回true, 否则返回false
     */
    public boolean deleteFile(File dirFile) {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {
            for (File file : dirFile.listFiles()) {
                deleteFile(file);
            }
        }

        return dirFile.delete();
    }
}
