package com.sina.app.bolt;

/**
 * Created by jingwei on 16/5/26.
 */
import java.io.IOException;
import com.sina.app.bolt.OperateTable;
public class HbaseTest {

    public static void main(String[] args) throws IOException {
        String[] columnFamilys = {"info","course"};
        OperateTable table = new OperateTable();
        try {
            table.addRow("user2", "1", "info", "info1", "1");
            table.addRow("user2","1","info","info2","2");
            table.addRow("user2","1","course","course1","1");
            table.getRow("user2","1");
        }catch (Exception e){
            System.out.println(e);
        }

//        String tableName = "user2";
//        try{
//            createTable(tableName,columnFamilys);
//        }catch(Exception e){
//            System.out.println("创建失败");
//        }
    }

}
