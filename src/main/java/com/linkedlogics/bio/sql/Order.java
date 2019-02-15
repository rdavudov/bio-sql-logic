package com.linkedlogics.bio.sql;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.linkedlogics.bio.dictionary.BioObj;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.object.BioTable;

public class Order {
	private List<String> tagList = new ArrayList<String>() ;
	private List<String> type = new ArrayList<String>() ;
	
	public Order orderAsc(String tag) {
		tagList.add(tag) ;
		type.add("asc") ;
		return this ;
	}
	
	public Order orderDesc(String tag) {
		tagList.add(tag) ;
		type.add("desc") ;
		return this ;
	}
	
	String getOrder(BioTable table) {
		StringBuilder order = new StringBuilder() ;
		String delimiter = "" ;
		for (int i = 0; i < tagList.size(); i++) {
			BioColumn column = table.getColumnByTag(tagList.get(i)) ;
			
			order.append(delimiter) ;
			if (column != null) {
				order.append(column.getColumn()).append(" ").append(type.get(i)) ;
			} else {
				order.append(tagList.get(i)).append(" ").append(type.get(i)) ;
			}
			delimiter = ", " ;
		}		
		return order.toString() ;
	}
}
