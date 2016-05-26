package com.sina.app.bolt.util;

import java.util.HashMap;
import java.util.Map.Entry;

import ea.proto.TableOuterClass.Table;
import ea.proto.TableOuterClass.Record;

/**
 * Store the counting data under a specific platformIdeaType and statType.
 * @author xiaocheng1
 *
 */
public class CountUnit {

	private final String platformIdeaType;
	private final String statType;
	
	class Key {
		public final String psid;
		public final String infos;
		public final String order;
		
		public final String comkey;
		
		public Key(String psid, String infos, String order) {
			this.psid = psid;
			this.infos = infos;
			this.order = order;
			this.comkey = new String(psid + "_" + infos + "_" + order);
		}
		
	    @Override
	    public boolean equals(Object obj) {
	    	if (this == obj) {
	    		return true;
	    	}
	        if(obj != null && obj instanceof Key) {
	            Key s = (Key) obj;
	            return comkey.equals(s.comkey);
	        }
	        return false;
	    }

	    @Override
	    public int hashCode() {
	        return comkey.hashCode();
	    }
	}
	
	class Count {
		public int pv = 0;
		public int click = 0;
	}

	private HashMap<Key, Count> value;

	public CountUnit(String platIdeaType, String statType) {
		this.platformIdeaType = platIdeaType;
		this.statType = statType;
		value = new HashMap<Key, Count>();
	}
	
	public int getPv(String psid, String infos, String order) {
		Key key = new Key(psid, infos, order);
		Count count = value.get(key);
		if (count != null) {
			return count.pv;
		}
		return 0;
	}

	public int getClick(String psid, String infos, String order) {
		Key key = new Key(psid, infos, order);
		Count count = value.get(key);
		if (count != null) {
			return count.click;
		}
		return 0;
	}
	
	public String getPlatformIdeaType() {
		return platformIdeaType;
	}
	
	public String getStatType() {
		return statType;
	}
	
	public void increment(String psid, String infos, String order, int isClick) {
		Key key = new Key(psid, infos, order);
		Count count = value.get(key);
		if (count != null) {
			if (isClick == 1) {
				count.click++;
			} else {
				count.pv++;
			}
		} else {
			count = new Count();
			if (isClick == 1) {
				count.click++;
			} else {
				count.pv++;
			}
			value.put(key, count);
		}
	}
	
	/**
	 * Convert to Table message.
	 * @return
	 */
	public Table toTable() {
		Table.Builder table = Table.newBuilder();
		table.setAdtype(platformIdeaType);
		table.setStattype(statType);
		table.setTs(System.currentTimeMillis() / 1000);
		
		Record.Builder record = Record.newBuilder();
		for (Entry<Key, Count> entry: value.entrySet()) {
			record.setPsid(entry.getKey().psid);
			record.setInfos(entry.getKey().infos);
			record.setOrder(entry.getKey().order);
			record.setPv(entry.getValue().pv);
			record.setClk(entry.getValue().click);
			table.addData(record.build());
			record.clear();
		}
		return table.build();
	}
	
	/**
	 * Get serialized table message.
	 * @return
	 */
	public byte[] toSerializedTable() {
		return toTable().toByteArray();
	}

	/**
	 * Get readable table message.
	 */
	public String toTableString() {
		return toTable().toString();
	}
	
	@Override
	public String toString() {
		return toTable().toString();
	}
}
