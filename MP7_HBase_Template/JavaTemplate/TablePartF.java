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

public class TablePartF{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER      

	
	String name = ???;
	String power = ???;
	String color = ???;

	String name1 = ???;
	String power1 = ???;
	String color1 = ???;
	System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", "+color);

   }
}
