package com.sina.app.bolt.util;

/**
 * Created by jingwei on 16/6/15.
 */

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Created by jingwei on 16/6/8.
 */
public class ClkWriteToHbase extends writeToHbase {
    public String row;
    public String val;
    boolean isExist;
    public ClkWriteToHbase(String tableColumn) {
        super(tableColumn);
        isExist = false;
    }
    public void setRowVal(String row){
       this.row = row;
    }
    public String pvFromHbase;

    public void askPv() {
        Get get = new Get(Bytes.toBytes(row));
        get.addColumn(Bytes.toBytes(tableFamily), Bytes.toBytes(tableColumn));
        try {
            Result result = table.table.get(get);
            if (result.isEmpty()) isExist = false;
            for(KeyValue kv:result.list()){
                pvFromHbase = new String(kv.getValue());
            }
            isExist = true;
        } catch (IOException e) {
            ResponseConverter.LOG.error("ask pv exist error {}", e);
            isExist= false;
        }
    }
}
