import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;
import java.util.*;

public class TablePartF{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER      
    HBaseConfiguration hBaseConfig = new HBaseConfiguration(new Configuration());
    HTable hTable = new HTable(hBaseConfig, "powers");

    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));
    scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));
    ResultScanner scanner = hTable.getScanner(scan);
    List<String> powerList = new ArrayList<>();
    List<String> nameList = new ArrayList<>();
    List<String> colorList = new ArrayList<>();
    List<Integer> iIndexList = new ArrayList<>();
    List<Integer> jIndexList = new ArrayList<>();

    for (Result result = scanner.next(); result != null; result = scanner.next()){
		String power = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
        String name = Bytes.toString(result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
        String color = Bytes.toString(result.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));

        powerList.add(power);
        nameList.add(name);
        colorList.add(color);
    }
    for(int i = 0; i < colorList.size(); i++) {
        for (int j = 0; j < colorList.size(); j++){
            if(colorList.get(i).equals(colorList.get(j)) && !nameList.get(i).equals(nameList.get(j))) {
                iIndexList.add(i);
                jIndexList.add(j);
            }
        }
    }
    for (int k = 0; k < iIndexList.size(); k++){
        String name = nameList.get(iIndexList.get(k));
        String power = powerList.get(iIndexList.get(k));
        String color = colorList.get(iIndexList.get(k));

        String name1 = nameList.get(jIndexList.get(k));
        String power1 = powerList.get(jIndexList.get(k));
        String color1 = colorList.get(jIndexList.get(k));
        System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", "+color);
    }


   }
}
