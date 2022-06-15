package parser.command;

import common.Database;
import common.Utility;

import java.io.*;
import java.util.Iterator;

/**
 * @author HP
 * @date 2022/5/20
 * show database;
 * show table;
 */
public class Show extends Command{

    public Show(String cmd) {
        super(cmd);
    }

    @Override
    public void analysis(){

        if (cmdWords.length != 2){
            System.out.println("语法错误！");
            return;
        }

        if (cmd.contains("database")){
            showDatabase();
        }else if (cmd.contains("table")){
            showTable();
        }else {
            System.out.println("语法错误!");
        }
    }

    private void showTable() {
        if (Utility.getNowPath().equals("") || Utility.getNowPath() == null){
            System.out.println("未选择数据库！");
            return;
        }
        Iterator<String> tables = Database.getCatalog().tableIdIterator();
        System.out.println("------table------");
        while (tables.hasNext()){
            System.out.println(tables.next());
        }
        System.out.println("-----------------");
        return;
    }

    private void showDatabase() {
        String txtFilePath = Utility.getPath() + "\\"+ "dbCatalog.txt";
        File file = new File(txtFilePath);
        if (!file.exists()) {
            System.out.println("dbCatalog未创建！");
            return;
        }

        try {
            /*FileInputStream fis = new FileInputStream(file);
            InputStreamReader reader = new InputStreamReader(fis);
            char[] buff = new char[1024];

            while (reader.read(buff) != -1){
                System.out.println(String.valueOf(buff));
            }*/

            String line = "";
            BufferedReader br = new BufferedReader(new FileReader(file));
            System.out.println("------database------");
            while ((line = br.readLine()) != null){
                System.out.println(line);
            }
            System.out.println("--------------------");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
