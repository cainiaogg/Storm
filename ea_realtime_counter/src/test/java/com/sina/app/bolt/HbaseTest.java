package com.sina.app.bolt;

/**
 * Created by jingwei on 16/5/26.
 */
import java.io.IOException;
import com.sina.app.bolt.OperateTable;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;
public class HbaseTest {
    public static void main(String[] args) throws Exception{
        OperateTable table = new OperateTable();
        String [] s = {
                "info","info1",
        };
        table.createTable("user5",s);
    }
}
