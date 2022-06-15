package parser.command;

import common.Database;
import common.Utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author HP
 * @date 2022/5/20
 */
public class Use extends Command{

    public Use(String cmd) {
        super(cmd);
    }

    /**
     * use database;
     * cmdWords[1]里面是数据库名
     */
    @Override
    public void analysis() {

        if (cmdWords.length != 2){
            System.out.println("语法错误！");
            return;
        }

        //先判断数据库是否存在
        String txtFilePath = Utility.getPath() + "\\"+ "dbCatalog.txt";
        File file = new File(txtFilePath);

        String allDatabases = "";
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            while ((line = br.readLine()) != null){
                allDatabases += line + " ";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (!allDatabases.contains(cmdWords[1])){
            System.out.println("当前数据库不存在！");
            return;
        }

        //设置当前路径为选择数据库的文件夹
        Utility.setNowPath(cmdWords[1]);

        //清空catalog和bufferpool 将选择数据库的表加载进去
        Database.reset();
        Database.getCatalog().loadSchema(Utility.getNowPath() + "\\" + "catalog.txt");
        System.out.println("database: "+ cmdWords[1] + " selected!");
    }
}
