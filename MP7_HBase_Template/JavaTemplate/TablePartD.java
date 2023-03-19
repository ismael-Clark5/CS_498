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
import org.apache.hadoop.hbase.client.Get;


import org.apache.hadoop.hbase.util.Bytes;

public class TablePartD{

   public static void main(String[] args) throws IOException {
	HBaseConfiguration hBaseConfig = new HBaseConfiguration(new Configuration());
	HTable hTable = new HTable(hBaseConfig, "powers");
	Get getRowOne = new Get(Bytes.toBytes("row1"));
	Get getRowNineteen = new Get(Bytes.toBytes("row19"));
	Result resultOne = table.get(getRowOne);
	Result resultNineteen = table.get(getRowNineteen);
	byte [] valueHero = resultOne.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	byte [] valuePower = resultOne.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power"));
	byte [] valueName = resultOne.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	byte [] valueXp = resultOne.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp"));
	byte [] valueColor = resultOne.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));

	
	String hero = Bytes.toString(valueHero);
	String power = Bytes.toString(valuePower);
	String name = Bytes.toString(valueName);
	String xp = Bytes.toString(valueXp);
	String color = Bytes.toString(valueColor);
	System.out.println("hero: "+hero+", power: "+power+", name: "+name+", xp: "+xp+", color: "+color);

	valueHero = resultNineteen.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	valueColor = resultNineteen.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
	hero = Bytes.toString(valueHero);
	color = Bytes.toString(valueColor);
	System.out.println("hero: "+hero+", color: "+color);

    valueHero = resultOne.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
    valueColor = resultOne.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
	valueName = resultOne.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	hero = Bytes.toString(valueHero);
	name = Bytes.toString(valueName);
	color = Bytes.toString(valueColor);
	System.out.println("hero: "+hero+", name: "+name+", color: "+color); 
   }
}

