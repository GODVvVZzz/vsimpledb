package parser.command;

import common.DbException;
import common.Utility;
import execution.Insert;
import storage.*;
import transaction.Transaction;
import transaction.TransactionAbortedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;

/**
 * @author HP
 * @date 2022/5/20
 * insert into table values(x,y,z,w);
 */
public class ComInsert extends Command{

    public ComInsert(String cmd) {
        super(cmd);
    }

    @Override
    public void analysis() {

        if (cmdWords.length != 4 || !cmdWords[1].equals("into") || !cmdWords[3].substring(0,6).equals("values")){
            System.out.println("语法错误！");
            return;
        }

        if (!dbAndTableCheck.check(cmdWords[2])){
            return;
        }

        //提取出两个括号之间的内容
        Pattern pattern = compile("\\(.*\\)");
        Matcher matcher = pattern.matcher(cmd);
        String values = "";
        if (matcher.find()){
            values = matcher.group(0);
        }
        values = values.substring(1,values.length() - 1);
        //提取插入的values
        String[] data = values.split(",");
        Double[] datas = new Double[4];
        for (int i = 0; i < datas.length; i++) {
            datas[i] = new Double(data[i]);
        }
        insertTuple(datas,cmdWords[2]);
    }


    public static void insertTuple(Double[] datas, String tableName){


        TupleDesc td = Utility.getTupleDesc(4,"double");
        Tuple tuple = new Tuple(td);
        tuple.setField(0,new DoubleField(datas[0]));
        tuple.setField(1,new DoubleField(datas[1]));
        tuple.setField(2,new DoubleField(datas[2]));
        tuple.setField(3,new DoubleField(datas[3]));

        List<Tuple> tupleList = new ArrayList<>();
        tupleList.add(tuple);
        TupleIterator insertTuples = new TupleIterator(td,tupleList);

        Transaction t = new Transaction();
        File file = new File(Utility.getNowPath() + "\\" + tableName + ".dat");
        int tableId = file.getAbsoluteFile().hashCode();
        Insert insOp = null;
        try {
            insOp = new Insert(t.getId(),insertTuples,tableId);
            t.start();
            insOp.open();
            if (insOp.hasNext()){
                insOp.next();
                t.commit();
                return;
            }
            System.out.println(tuple + " 插入失败！");
        } catch (DbException | TransactionAbortedException | IOException e) {
            e.printStackTrace();
        }finally {
            insOp.close();
        }
    }
}
