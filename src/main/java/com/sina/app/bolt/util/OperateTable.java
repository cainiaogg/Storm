package com.sina.app.bolt.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
    private static Configuration conf;
    private static String tableName = "ns_hero:sinaad_rtlabel";
    public HTable table;
    static{
        String relativelyPath=System.getProperty("user.dir");
        String filePath = relativelyPath+"/hbase-site.xml";
        Path path = new Path(filePath);
        conf = new Configuration();
        conf.addResource(path);
        conf = HBaseConfiguration.create(conf);
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
