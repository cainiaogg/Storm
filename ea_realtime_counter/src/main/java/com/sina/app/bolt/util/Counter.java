package com.sina.app.bolt.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import ea.proto.TableOuterClass.Table;

/**
 * Store counting data.
 * @author xiaocheng1
 *
 */
public class Counter {

	// <platformIdeaType, <statType, countingInfo>>
	private final HashMap<String, HashMap<String, CountUnit>> data;
	
	public Counter() {
		data = new HashMap<String, HashMap<String, CountUnit>>();
	}
	
	public void increment(String platIdeaType, String statType, String psid, String infos, String order, int isClick) {
		HashMap<String, CountUnit> inner = data.get(platIdeaType);
		if (inner != null) {
			CountUnit unit = inner.get(statType);
			if (unit != null) {
				unit.increment(psid, infos, order, isClick);
			} else {
				unit = new CountUnit(platIdeaType, statType);
				unit.increment(psid, infos, order, isClick);
				inner.put(statType, unit);
			}
		} else {
			CountUnit unit = new CountUnit(platIdeaType, statType);
			unit.increment(psid, infos, order, isClick);
			inner = new HashMap<String, CountUnit>();
			inner.put(statType, unit);
			data.put(platIdeaType, inner);
		}
	}
	
	public void clear() {
		data.clear();
	}
	
	public ArrayList<Table> getCountingResult() {
		ArrayList<Table> ret = new ArrayList<Table>();
		for (String ptype: data.keySet()) {
			for (Entry<String, CountUnit> entry: data.get(ptype).entrySet()) {
				ret.add(entry.getValue().toTable());
			}
		}
		return ret;
	}
	
	public boolean hasData() {
		return data.size() > 0;
	}
	
	public void print() {
		for (String key: data.keySet()) {
			for (Entry<String, CountUnit> entry: data.get(key).entrySet()) {
				System.out.println(key + "\t" + entry.getKey() + "\t" + entry.getValue());
			}
		}
	}

}
