package parser.command;

import common.Database;
import common.DbException;
import common.Utility;
import execution.Delete;
import execution.Filter;
import execution.IndexPredicate;
import execution.Predicate;
import index.BTreeScan;
import storage.DoubleField;
import storage.Field;
import storage.IntField;
import storage.Tuple;
import transaction.Transaction;
import transaction.TransactionAbortedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author HP
 * @date 2022/5/20
 * delete from nuaa where x=1;
 * 1.先查找 再删除
 * 2.有索引和无索引的查找
 */
public class ComDelete extends Command{
    int tableId;
    public ComDelete(String cmd) {
        super(cmd);
    }

    @Override
    public void analysis() {

        if (cmdWords.length != 5 || !cmdWords[1].equals("from") || !cmdWords[3].equals("where")){
            System.out.println("语法错误！");
            return;
        }

        if (!dbAndTableCheck.check(cmdWords[2])){
            return;
        }

        File file = new File(Utility.getNowPath() + "\\" + cmdWords[2] + ".dat");
        tableId = file.getAbsoluteFile().hashCode();

        if (cmdWords[4].startsWith("x")){
            indexDelete();
        }else {
            nonIndexDelete();
        }
    }

    private void nonIndexDelete() {
        Double value = new Double(cmdWords[4].substring(2));
        int filedNum = -1;
        if (cmdWords[4].startsWith("y")){
            filedNum = 1;
        }else if (cmdWords[4].startsWith("z")){
            filedNum = 2;
        }else if (cmdWords[4].startsWith("w")){
            filedNum = 3;
        }
        Field field = new DoubleField(value);

        Transaction t = new Transaction();
        String op = cmdWords[4].substring(1,2);
        BTreeScan seqScan = new BTreeScan(t.getId(),tableId,"",null);
        Predicate predicate = new Predicate(filedNum,Utility.stringToOp(op),field);
        Filter it = new Filter(predicate,seqScan);

        Delete delete = new Delete(t.getId(),it);
        try {
            t.start();
            delete.open();
            Tuple result = delete.next();
            int cnt = ((IntField)result.getField(0)).getValue();
            System.out.println("delete " + cnt + " rows");
            delete.close();
            t.commit();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void indexDelete() {
        Double value = new Double(cmdWords[4].substring(2));
        Field field = new DoubleField(value);

        String op = cmdWords[4].substring(1,2);
        IndexPredicate indexPre = new IndexPredicate(Utility.stringToOp(op),field);
        Transaction t = new Transaction();
        BTreeScan it = new BTreeScan(t.getId(),tableId,"",indexPre);

        Delete delete = new Delete(t.getId(),it);
        try {
            t.start();
            delete.open();
            Tuple result = delete.next();
            int cnt = ((IntField)result.getField(0)).getValue();
            System.out.println("delete " + cnt + " rows");
            delete.close();
            t.commit();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
