package com.sina.app.bolt.util;

import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.hbase.ipc.RpcClient.LOG;

/**
 * Created by jingwei on 16/6/16.
 */
public class AskFromHbase {
    public ClkWriteToHbase clkWriteToHbase;
    public AskFromHbase(String row,String tableColumn){
        clkWriteToHbase = new ClkWriteToHbase(tableColumn);
        clkWriteToHbase.setRowVal(row);

    }
    public boolean askExist(){
        Object ret = null;
        try{
            ret = UserGroupInformation.createRemoteUser("hero").doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception{
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
