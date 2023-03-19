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
import java.utils.*;
public class TablePartF{
    public class HeroData {
        String name;
        String power;
        String color;
    }
   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER      
    HBaseConfiguration hBaseConfig = new HBaseConfiguration(new Configuration());
    HTable hTable = new HTable(hBaseConfig, "powers");
    List<HeroData> listOfHeroes = new ArrayList<>();
    int maxRows = 26;
    int rowCount = 0;
    for (int rowCount = 1; rowCount < maxRows; rowCount++){
        String rowName = "row" + rowCount;
        Get getRow = new Get(Bytes.toBytes(rowName));
        Result result = hTable.get(getRow);
        byte [] valuePower = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power"));
        byte [] valueName = resultOne.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
        byte [] valueColor = resultOne.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
        HeroData hero = new HeroData();
        hero.name = valueName;
        hero.power = valuePower;
        hero.color = valueColor;
        listOfHeroes.add(hero);
    }

	String name = "???";
	String power = "???";
	String color = "???";

	String name1 = "???";
	String power1 = "???";
	String color1 = "???";
	System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", "+color);

   }
}
