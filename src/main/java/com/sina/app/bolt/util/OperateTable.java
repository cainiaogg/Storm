package com.sina.app.bolt.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.FileSystem.LOG;

/**
 * Created by jingwei on 16/5/27.
 */
public class OperateTable{
    private static Configuration conf = HBaseConfiguration.create();
    private static String tableName = "sinaad_rtlabel";
    public HTable table;
    static{
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public OperateTable(){
        try {
            table = new HTable(conf, tableName);
        }catch(Exception e){
            LOG.error("new HTable Wrong of {}",e);
        }
    }
    public void addRows(List<Put> putList)throws Exception{
        table.batch(putList);
    }
}
