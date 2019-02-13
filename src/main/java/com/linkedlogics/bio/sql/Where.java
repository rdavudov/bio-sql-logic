package com.linkedlogics.bio.sql;

import java.sql.Types;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.linkedlogics.bio.BioExpression;
import com.linkedlogics.bio.BioObject;
import com.linkedlogics.bio.sql.exception.SqlException;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.object.BioTable;
import com.linkedlogics.bio.sql.utility.SqlUtility;

public class Where {
	protected String where ;
	protected HashMap<Integer, Object> valueMap ;
	protected HashMap<Integer, Integer> typeMap ;
	
	/**
	 * Default constructor with only where string
	 * @param where
	 */
	public Where(String where) {
		this(where, new HashMap<Integer, Object>(), new HashMap<Integer, Integer>()) ;
	}
	
	/**
	 * Constructs where using provided where string and values/types
	 * @param where
	 * @param valueMap
	 * @param typeMap
	 */
	public Where(String where, HashMap<Integer, Object> valueMap, HashMap<Integer, Integer> typeMap) {
		this.where = where ;
		this.valueMap = valueMap ;
		this.typeMap = typeMap ;
	}
	
	/**
	 * Constructs where clause based on keys inside bio object
	 * @param object
	 * @param table
	 */
	public Where(BioObject object, BioTable table) {
    	valueMap = new HashMap<Integer, Object>() ;
    	typeMap = new HashMap<Integer, Integer>() ;
    	AtomicInteger index = new AtomicInteger(1) ;
    	this.where = object.stream().map(e -> {
    		BioColumn column = table.getColumnByTag(e.getKey()) ;
    		if (column != null) {
    			Object value = e.getValue() ;
                 if (value instanceof BioExpression) {
                	throw new SqlException("bio object contains bio expressions please first run fill() to evaluate them"); 
                 }
                 typeMap.put(index.get(), SqlUtility.getSqlType(e.getValue())) ;
    			 valueMap.put(index.get(), value) ;
                 index.incrementAndGet();
                 return column.getColumn() + " = ? " ;
    		}
    		return null ;
    	}).filter(c -> {
    		return c != null ;
    	}).collect(Collectors.joining(" and "));
	}
	
	public Where setInt(int index, Object value) {
		valueMap.put(index, value) ;
		typeMap.put(index, Types.INTEGER) ;
		return this ;
	}
	
	public Where setBoolean(int index, Object value) {
		valueMap.put(index, value) ;
		typeMap.put(index, Types.INTEGER) ;
		return this ;
	}
	
	public Where setLong(int index, Object value) {
		valueMap.put(index, value) ;
		typeMap.put(index, Types.NUMERIC) ;
		return this ;
	}
	
	public Where setDouble(int index, Object value) {
		valueMap.put(index, value) ;
		typeMap.put(index, Types.DOUBLE) ;
		return this ;
	}
	
	public Where setDate(int index, Object value) {
		valueMap.put(index, value) ;
		typeMap.put(index, Types.DATE) ;
		return this ;
	}
	
	public Where setDateTime(int index, Object value) {
		valueMap.put(index, value) ;
		typeMap.put(index, Types.TIMESTAMP) ;
		return this ;
	}
	
	public Where setString(int index, Object value) {
		valueMap.put(index, value) ;
		typeMap.put(index, Types.VARCHAR) ;
		return this ;
	}
	
	public Where setObject(int index, Object value, int sqlType) {
		valueMap.put(index, value) ;
		typeMap.put(index, sqlType) ;
		return this ;
	}
	
	public Where setNull(int index, int sqlType) {
		valueMap.put(index, null) ;
		typeMap.put(index, sqlType) ;
		return this ;
	}
	
	public HashMap<Integer, Object> getValueMap() {
		return valueMap;
	}
	
	public void setValueMap(HashMap<Integer, Object> valueMap) {
		this.valueMap = valueMap;
	}

	public HashMap<Integer, Integer> getTypeMap() {
		return typeMap;
	}
	
	public void setTypeMap(HashMap<Integer, Integer> typeMap) {
		this.typeMap = typeMap;
	}
	
	public int getType(int index) {
		return getTypeMap().get(index) ;
	}
	
	public Object getValue(int index) {
		return getValueMap().get(index) ;
	}
	
	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public Where merge(Where where) {
		if (where == null) {
			return this ;
		}
		
		Where merged = new Where(getWhere() + " and " + where.getWhere()) ;
		merged.setTypeMap(new HashMap<Integer, Integer>(getTypeMap()));
		merged.setValueMap(new HashMap<Integer, Object>(getValueMap()));
		int size = merged.getTypeMap().size() ;
		for (int i = 1; i <= where.getTypeMap().size(); i++) {
			merged.getTypeMap().put(i + size, where.getTypeMap().get(i)) ;
			merged.getValueMap().put(i + size, where.getValueMap().get(i)) ;
		}
		
		return merged ;
	}
	
	public String toString() {
		return where ;
	}
	
	public int size() {
		return valueMap.size() ;
	}
}
