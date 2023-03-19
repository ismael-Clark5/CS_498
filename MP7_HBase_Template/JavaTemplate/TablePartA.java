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

public class TablePartA{

   public static void main(String[] args) throws IOException {

	HBaseConfigurtation hBaseConfig = new HBaseConfiguration(new Configuration());
    HTableDescriptor powersTable = new HTableDescriptor("powers");
    powersTable.addFamily(new HColumnDescriptor("personal"));
    powersTable.addFamily(new HColumnDescriptor("professional"));
    powersTable.addFamily(new HColumnDescriptor("custom"));

    HTableDescriptor foodTable = new HTableDescriptor("food");
    foodTable.addFamily(new HColumnDescriptor("nutrition"));
    foodTable.addFamily(new HColumnDescriptor("taste"));

    HBaseAdmin hba = new HBaseAdmin(hBaseConfig);
    hba.createTable(ht);
   }
}

