package com.sina.app.bolt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Created by jingwei on 16/5/26.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sina.app.bolt.OperateTable;
import org.apache.hadoop.ipc.UnexpectedServerException;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import com.sun.security.auth.callback.TextCallbackHandler;
import java.io.File;
import javax.security.auth.Subject;
import java.security.PrivilegedAction;
public class HbaseTest {
    public static void main(String [] args){
        String relativelyPath=System.getProperty("user.dir");
        System.out.println(relativelyPath);
    }
}