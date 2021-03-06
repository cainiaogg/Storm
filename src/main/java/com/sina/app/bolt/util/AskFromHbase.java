package com.sina.app.bolt.util;

import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.hbase.ipc.RpcClient.LOG;

/**
 * Created by jingwei on 16/6/16.
 */
public class AskFromHbase {
    public ClkWriteToHbase clkWriteToHbase;
    public String row;
    public String tableColumn;
    public AskFromHbase(String row,String tableColumn){
        this.row =row;
        this.tableColumn = tableColumn;

    }
    public boolean askExist(){
        Object ret = null;
        try{
            ret = UserGroupInformation.createRemoteUser("hero").doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception{
                    clkWriteToHbase = new ClkWriteToHbase(tableColumn);
                    clkWriteToHbase.setRowVal(row);
                    clkWriteToHbase.askPv();
                    return null;
                }
            });
        }catch(Exception e){
            LOG.error("send userImf error {}",e);
        }
        return clkWriteToHbase.isExist;
    }
}
