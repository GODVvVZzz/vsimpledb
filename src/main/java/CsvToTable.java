import common.Database;
import common.Utility;
import org.junit.Test;
import parser.command.ComInsert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author HP
 * @date 2022/5/23
 * 将一个csv文件转换为一个数据库中的表
 * csvPath : 对应csv文件地址
 * tableName ：对应数据库中的表
 */
public class CsvToTable {
    public static void main(String[] args) {

        //要转换的csv地址
        String csvPath = "E:\\南航\\数据库实验\\NCAA2022\\4.csv";

        //设置数据库
        Utility.setNowPath("NUAA");
        Database.getCatalog().loadSchema(Utility.getNowPath() + "\\catalog.txt");

        //设置被插入的表
        String tableName = "4";

        File csv = new File(csvPath);

        try {
            BufferedReader textFile = new BufferedReader(new FileReader(csv));
            String line = "";

            //去掉X,Y,Z,W列头
            textFile.readLine();
            int cnt = 0;
            while ((line = textFile.readLine()) != null){
                String[] data = line.split(",");
                if (data.length == 3){
                    String[] newData = new String[4];
                    for (int i = 0; i < 3; i++) {
                        newData[i] = data[i];
                    }
                    newData[3] = "";
                    data = newData;
                }
                Double[] insertData = meanCompletion(data);
                ComInsert.insertTuple(insertData,tableName);
                cnt++;
            }
            System.out.println("共插入 " + cnt + " 条记录！");

            textFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 采用平均值插补法进行数据补全
     * @param data
     * @return
     */
    public static Double[] meanCompletion(String[] data){

        HashMap<Integer,Double> nonNullValues = new HashMap<>(4);
        ArrayList<Integer> emptyNum = new ArrayList<>();
        Double[] values = new Double[4];


        for (int i = 0; i < 4; i++) {
            String s = data[i];
            if (s != null && !s.isEmpty()){
                nonNullValues.put(i,new Double(s));
            }else {
                emptyNum.add(i);
            }
        }

        //没空
        if (emptyNum.size() == 0){
            for (int i = 0; i < 4; i++) {
                values[i] = new Double(data[i]);
            }
        }

        //缺一个值
        else if (emptyNum.size() == 1){
            double sum = 0;
            for (Integer index : nonNullValues.keySet()) {
                values[index] = nonNullValues.get(index);
                sum += values[index];
            }
            values[emptyNum.get(0)] = sum/3.0;
        }

        //缺两个值
        else if (emptyNum.size() == 2){
            double sum = 0;
            for (Integer index : nonNullValues.keySet()) {
                values[index] = nonNullValues.get(index);
                sum += values[index];
            }
            values[emptyNum.get(0)] = sum/2.0;
            values[emptyNum.get(1)] = sum/2.0 + 1;
        }

        //缺三个值
        else if (emptyNum.size() == 3){
            double sum = 0;
            for (Integer index : nonNullValues.keySet()) {
                values[index] = nonNullValues.get(index);
                sum += values[index];
            }
            values[emptyNum.get(0)] = sum;
            values[emptyNum.get(1)] = sum + 1;
            values[emptyNum.get(2)] = sum + 2;
        }

        return values;
    }

    @Test
    public void nullTest(){
        String csvPath = "E:\\南航\\数据库实验\\NCAA2022\\test.csv";

        File csv = new File(csvPath);

        try {
            BufferedReader textFile = new BufferedReader(new FileReader(csv));
            String line = "";

            while ((line = textFile.readLine()) != null){
                System.out.println(line);
                String[] data = line.split(",");
                System.out.println(data.length);
                for (String datum : data) {
                    System.out.print(datum);
                }
                System.out.println("\n");
            }

            textFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
