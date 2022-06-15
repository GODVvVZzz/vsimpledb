package parser.command;

import common.DbException;
import common.Utility;
import execution.Filter;
import execution.IndexPredicate;
import execution.OpIterator;
import execution.Predicate;
import index.BTreeScan;
import storage.DoubleField;
import storage.Field;
import transaction.Transaction;
import transaction.TransactionAbortedException;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

/**
 * @author HP
 * @date 2022/5/20
 * 支持3种命令
 * 1.一个参数  索引字段的查询   只支持 大于 小于 等于
 * 2.一个参数  非索引字段的查询
 * 3.一个窗口  先查询索引字段，再依次筛选。
 * select * from 4 where x=？;
 * select * from 4 where x=[0,1],y=[0,1],z=[0,1],w=[0,1];
 */
public class Select extends Command{

    int tableId;
    public Select(String cmd) {
        super(cmd);
    }


    @Override
    public void analysis() {

        if (cmdWords.length != 6 || !cmdWords[1].equals("*") || !cmdWords[2].equals("from")
            || !cmdWords[4].equals("where")){
            System.out.println("语法错误！");
            return;
        }

        if (!dbAndTableCheck.check(cmdWords[3])){
            return;
        }

        File file = new File(Utility.getNowPath() + "\\" + cmdWords[3] + ".dat");
        tableId = file.getAbsoluteFile().hashCode();

        if (cmdWords[5].length() > 25){
            windowsSelect();
        }else {
            if (cmdWords[5].startsWith("x")){
                indexSelect();
            }else {
                nonIndexSelect();
            }
        }
    }


    private void indexSelect() {
        Double value = new Double(cmdWords[5].substring(2));
        Field field = new DoubleField(value);


        String op = cmdWords[5].substring(1,2);
        IndexPredicate indexPre = new IndexPredicate(Utility.stringToOp(op),field);
        Transaction t = new Transaction();
        BTreeScan it = new BTreeScan(t.getId(),tableId,"",indexPre);
        t.start();
        try {
            it.open();
            System.out.println("x \t\t\t y \t\t\t z \t\t\t w");
            System.out.println("-------------------------------------------");
            int cnt = 0;
            while (it.hasNext()){
                System.out.println(it.next());
                cnt++;
            }
            System.out.println("-------------------------------------------");
            System.out.println(cnt + " rows.");
            it.close();
            t.commit();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void nonIndexSelect() {
        Double value = new Double(cmdWords[5].substring(2));
        int filedNum = -1;
        if (cmdWords[5].startsWith("y")){
            filedNum = 1;
        }else if (cmdWords[5].startsWith("z")){
            filedNum = 2;
        }else if (cmdWords[5].startsWith("w")){
            filedNum = 3;
        }
        Field field = new DoubleField(value);

        Transaction t = new Transaction();
        String op = cmdWords[5].substring(1,2);
        BTreeScan seqScan = new BTreeScan(t.getId(),tableId,"",null);
        Predicate predicate = new Predicate(filedNum,Utility.stringToOp(op),field);
        Filter it = new Filter(predicate,seqScan);

        t.start();
        try {
            it.open();
            System.out.println("x \t\t\t y \t\t\t z \t\t\t w");
            System.out.println("-------------------------------------------");
            int cnt = 0;
            while (it.hasNext()){
                System.out.println(it.next());
                cnt++;
            }
            System.out.println("-------------------------------------------");
            System.out.println(cnt + " rows.");
            it.close();
            t.commit();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * select * from 4 where x=[0,1],y=[0,1],z=[0,1],w=[0,1];
     * select * from 4 where x=[0,100000],y=[0,100000],z=[0,100000],w=[0,100000];
     */
    private void windowsSelect() {

        Pattern pattern = compile("(?<=\\[)[^\\]]+");
        Matcher matcher = pattern.matcher(cmd);
        String[] values = new String[4];
        int i = 0;
        while (matcher.find()){
            values[i] = matcher.group();
            i++;
        }
        String[] stringDataX = values[0].split(",");
        String[] stringDataY = values[1].split(",");
        String[] stringDataZ = values[2].split(",");
        String[] stringDataW = values[3].split(",");

        Double xDown = new Double(stringDataX[0]);
        Double xUp = new Double(stringDataX[1]);
        Double yDown = new Double(stringDataY[0]);
        Double yUp = new Double(stringDataY[1]);
        Double zDown = new Double(stringDataZ[0]);
        Double zUp = new Double(stringDataZ[1]);
        Double wDown = new Double(stringDataW[0]);
        Double wUp = new Double(stringDataW[1]);

        Field fieldXD = new DoubleField(xDown);
        Field fieldXU = new DoubleField(xUp);
        Field fieldYD = new DoubleField(yDown);
        Field fieldYU = new DoubleField(yUp);
        Field fieldZD = new DoubleField(zDown);
        Field fieldZU = new DoubleField(zUp);
        Field fieldWD = new DoubleField(wDown);
        Field fieldWU = new DoubleField(wUp);

        Transaction t = new Transaction();
        t.start();

        IndexPredicate indexPreXD = new IndexPredicate(Predicate.Op.GREATER_THAN_OR_EQ,fieldXD);
        BTreeScan filterXD = new BTreeScan(t.getId(),tableId,"",indexPreXD);

        OpIterator filterXU = tempFilter(Predicate.Op.LESS_THAN,fieldXU,0,filterXD);

        OpIterator filterYD = tempFilter(Predicate.Op.GREATER_THAN_OR_EQ,fieldYD,1,filterXU);
        OpIterator filterYU = tempFilter(Predicate.Op.LESS_THAN,fieldYU,1,filterYD);
        OpIterator filterZD = tempFilter(Predicate.Op.GREATER_THAN_OR_EQ,fieldZD,2,filterYU);
        OpIterator filterZU = tempFilter(Predicate.Op.LESS_THAN,fieldZU,2,filterZD);
        OpIterator filterWD = tempFilter(Predicate.Op.GREATER_THAN_OR_EQ,fieldWD,3,filterZU);
        OpIterator filterWU = tempFilter(Predicate.Op.LESS_THAN,fieldWU,3,filterWD);

        try {
            filterWU.open();
            System.out.println("x \t\t\t y \t\t\t z \t\t\t w");
            System.out.println("-------------------------------------------");
            int cnt = 0;
            while (filterWU.hasNext()){
                System.out.println(filterWU.next());
                cnt++;
            }
            System.out.println("-------------------------------------------");
            System.out.println(cnt + " rows.");
            filterWU.close();
            t.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private OpIterator tempFilter(Predicate.Op op, Field field, int filedNum,OpIterator opIterator){

        Predicate predicate = new Predicate(filedNum,op,field);
        Filter it = new Filter(predicate,opIterator);

        return it;
    }
}
