package parser.command;

import common.Database;
import common.Utility;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author HP
 * @date 2022/5/23
 */
public class dbAndTableCheck {

    public static boolean check(String tableName){
        if (Utility.getNowPath().equals("") || Utility.getNowPath().isEmpty()){
            System.out.println("请先选择数据库！");
            return false;
        }

        Iterator<String> tables = Database.getCatalog().tableIdIterator();
        ArrayList<String> names = new ArrayList<>();
        while (tables.hasNext()){
            names.add(tables.next());
        }
        if (!names.contains(tableName)){
            System.out.println("此表在当前数据库中不存在！");
            return false;
        }

        return true;
    }
}
