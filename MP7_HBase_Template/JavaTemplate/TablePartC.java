import java.io.IOException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;

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

import java.io.*;

public class TablePartC{

   public static void main(String[] args) throws IOException {

    HBaseConfiguration hBaseConfig = new HBaseConfiguration(new Configuration());
    HBaseAdmin hba = new HBaseAdmin(hBaseConfig);
    HTable hTable = new HTable(hBaseConfig, "powers");
    HColumnDescriptor powersPersonalDescriptor = new HColumnDescriptor("personal");
    HColumnDescriptor powersProfessionalDescriptor = new HColumnDescriptor("professional");
    HColumnDescriptor powersCustomDescriptor = new HColumnDescriptor("custom");

    hba.addColumn("hero", powersPersonalDescriptor);
    hba.addColumn("power", powersPersonalDescriptor);

    hba.addColumn("name", powersProfessionalDescriptor);
    hba.addColumn("xp", powersProfessionalDescriptor);

    hba.addColumn("color", powersCustomDescriptor);

    File csvInput = new File("input.csv");
    FileReader fr = new FileReader(csvInput);
    BufferedReader br = new BufferedReader(fr);
    String line = "";
    String delimiter = ",";
    Put p;
    while((line = br.readLine()) != null){
        String [] elements = line.trim().split(delimiter);
        String rowName = elements[0];
        String hero = elements[1];
        String power = elements[2];
        String name = elements[3];
        String xp = elements[4];
        String color = elements[5];

        p = new Put(Bytes.toBytes(rowName));
        p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes(hero));
        p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes(power));

        p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes(name));
        p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes(name));

        p.add(Bytes.toBytes("custom"), Bytes.toBytes("color"), Bytes.toBytes(color));

        hTable.put(p);
    }
    hTable.close();
   }
}

