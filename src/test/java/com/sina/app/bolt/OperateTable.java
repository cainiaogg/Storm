package com.sina.app.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingwei on 16/5/27.
 */
public class OperateTable {
    private static Configuration conf = HBaseConfiguration.create();
    static{
        conf.set("hbase.zookeeper.quorum","10.39.6.87");
    }
    public static void scanTables() throws Exception{
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        HTableDescriptor []hTableDescriptor= hAdmin.listTables();
        for(HTableDescriptor tmp:hTableDescriptor){
            System.out.println(tmp.getNameAsString());
        }

    }
    public static int createTable(String tableName,String [] columnFamilys)throws Exception{
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if(hAdmin.tableExists(tableName)){
//            System.out.println("表已存在");
//            System.exit(0);
            return 0;
        }else{

            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (String columnFamily:columnFamilys){
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }
            hAdmin.createTable(tableDesc);
//            System.out.println("创建成功");
            return 1;
        }
    }
    public static void deleteTable(String tableName)throws Exception{
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if(hAdmin.tableExists(tableName)){
            hAdmin.disableTable(tableName);
            //shutdown a table
            hAdmin.deleteTable(tableName);
            System.out.println("删除成功");
        }
        else{
            System.out.println("删除的表不存在");
            System.exit(0);
        }
    }
    public static void addRow(String tableName,String row,String columnFamily,String column,String value)throws Exception{
        HTable table = new HTable(conf,tableName);
        Put put = new Put(Bytes.toBytes(row));
        //row bytes array
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
    }

    public static void delRow(String tableName,String row)throws Exception{
        HTable table = new HTable(conf,tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }
    public static void delMultiRows(String tableName,String [] rows) throws Exception{
        HTable table = new HTable(conf,tableName);
        List<Delete> list= new ArrayList<Delete>();
        for(String row:rows){
            Delete del = new Delete(Bytes.toBytes(row));
            list.add(del);
        }
        table.delete(list);
    }
    public static void getRow(String tableName,String row) throws Exception{
        HTable table = new HTable(conf,tableName);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        for(KeyValue rowKV:result.raw()){
            System.out.println("row = "+new String(rowKV.getRow()));
            System.out.println("column family = "+new String(rowKV.getFamily()));
            System.out.println("value ="+new String(rowKV.getValue()));
        }
    }
}
