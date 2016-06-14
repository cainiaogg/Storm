package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.hadoop.hbase.ipc.RpcClient.LOG;

public class ClickLog extends FormatLog{
	public int clickLength = 17;
	public boolean isValid = true;
	public String uuid;
	public String logclkVal;
	public String timeSign;
	public static final String tableCloumn = "logclk"; //lie
	public String toKafka;
	public ClickLog(String log) {
		String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
		if (segs.length < clickLength) {
			isValid = false;
			return;
		}
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try{
			Date date = simpleDateFormat.parse(segs[0]);
		}catch(Exception e){
			isValid = false;
			LOG.error("ClickLog isValid",e);
			return ;
		}
		timeSign = segs[0];
		uuid = segs[3];
		int valHash = getHash(uuid);
		if(valHash >= sampleCnt) {
			isValid = false;
			return ;
		}
		logclkVal = log;
	}
	public boolean getFromHbase(){
		OperateTable table = new OperateTable();
		Get get = new Get(Bytes.toBytes(uuid));
		get.addColumn(Bytes.toBytes(tableFamily),Bytes.toBytes(tableCloumn));
		try{
			Result result = table.table.get(get);
			if(result.isEmpty()) return false;
			for(KeyValue kv:result.list()){
				toKafka = new String(kv.getValue());
			}
			return true;
		}catch(Exception e){
			LOG.error("getFromHbase error {}",e);
			return false;
		}

	}
}
