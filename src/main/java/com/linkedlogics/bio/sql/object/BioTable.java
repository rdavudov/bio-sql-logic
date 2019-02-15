package com.linkedlogics.bio.sql.object;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.linkedlogics.bio.BioDictionary;
import com.linkedlogics.bio.dictionary.BioObj;
import com.linkedlogics.bio.sql.Where;
import com.linkedlogics.bio.sql.utility.SqlUtility;

/**
 * Bio table definition object represents a table
 * @author rdavudov
 *
 */
public class BioTable {
	private BioObj obj ;
	private String schema ;
	private String table ;
	private int code ;
	private int dictionary ;
	
	private BioColumn[] keys ;
	private BioColumn versionColumn ;
	private BioColumn[] columns ;
	private HashMap<String, BioColumn> columnByTagMap = new HashMap<String, BioColumn>() ;
	private HashMap<String, BioColumn> columnByNameMap = new HashMap<String, BioColumn>() ;
	
	private String insert ;
	private String update ;
	private String delete ;
	private String select ;
	private String count ;
	private Where where ;
	private Where whereWithVersion ;
	private HashMap<String, BioRelation> relationByTagMap = new HashMap<String, BioRelation>() ;
	private List<BioRelation> relations = new ArrayList<BioRelation>() ;
	
	public BioTable(int dictionary, int code) {
		this(dictionary, code, null, null) ;
	}
	
	public BioTable(int dictionary, int code, String table) {
		this(dictionary, code, table, null) ;
	}
	
	public BioTable(int dictionary, int code, String table, String schema) {
		this.dictionary = dictionary ;
		this.code = code ;
		this.schema = schema  ;
		this.table = table ;
		this.obj = BioDictionary.getDictionary(dictionary).getObjByCode(code) ;
		if (this.table == null) {
			this.table = this.obj.getName() ;
		}
	}
	
	public BioTable(BioObj obj, String table, String schema) {
		this.dictionary = obj.getDictionary() ;
		this.code = obj.getCode() ;
		this.schema = schema  ;
		this.table = table ;
		this.obj = obj ;
		if (this.table == null) {
			this.table = this.obj.getName() ;
		}
	}
	
	public BioColumn[] getKeys() {
		return keys ;
	}
	
	public BioColumn getVersionColumn() {
		return versionColumn ;
	}
	
	public BioColumn[] getColumns() {
		return columns;
	}
	
	public void addColumn(BioColumn column) {
		// sometimes it can be custom column besides tag
		// in this case tag will be null
		if (column.getTag() != null) 
			this.columnByTagMap.put(column.getTag().getName(), column) ;
		else 
			this.columnByTagMap.put(column.getColumn(), column) ;
	}
	
	public void addRelation(BioRelation relation) {
		this.relationByTagMap.put(relation.getTag().getName(), relation) ;
	}
	
	public List<BioRelation> getRelations() {
		return relations;
	}
	
	public int getCode() {
		return code;
	}

	public int getDictionary() {
		return dictionary;
	}

	public void generate() {
		this.columns = new BioColumn[this.columnByTagMap.size()] ;
		int index = 0 ;
		ArrayList<BioColumn> keyList = new ArrayList<BioColumn>() ;
		for (Entry<String, BioColumn> e : this.columnByTagMap.entrySet()) {
			this.columnByNameMap.put(e.getValue().getColumn(), e.getValue()) ;
			this.columns[index] = e.getValue() ;
			if (e.getValue().isKey()) {
				keyList.add(e.getValue()) ;
			}
			if (e.getValue().isVersion()) {
				this.versionColumn = e.getValue() ;
			}
			index++ ;
		}
		
		this.keys = new BioColumn[keyList.size()] ;
		keyList.toArray(this.keys) ;
		
		this.insert = SqlUtility.generateInsert(this) ;
		this.update = SqlUtility.generateUpdate(this) ;
		this.delete = SqlUtility.generateDelete(this) ;
		this.select = SqlUtility.generateSelect(this) ;
		this.count = SqlUtility.generateCount(this) ;
		this.where = SqlUtility.generateWhere(this) ;
		this.whereWithVersion = SqlUtility.generateWhereWithVersion(this) ;
	}
	
	public void generateRelations() {
		for (Entry<String, BioRelation> r : this.relationByTagMap.entrySet()) {
			this.relations.add(r.getValue()) ;
		}
		
		for (BioRelation r : this.relations) {
			r.setWhere(SqlUtility.generateWhereRelation(r));
		}
	}
	
	public BioColumn getColumnByTag(String tag) {
		return columnByTagMap.get(tag) ;
	}
	
	public BioColumn getColumnByName(String column) {
		return columnByNameMap.get(column) ;
	}
	
	public BioObj getObj() {
		return obj;
	}

	public void setObj(BioObj obj) {
		this.obj = obj;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getInsert() {
		return insert;
	}

	public String getUpdate() {
		return update;
	}

	public String getDelete() {
		return delete;
	}

	public String getSelect() {
		return select;
	}

	public String getCount() {
		return count;
	}

	public Where getWhere() {
		return where;
	}

	public Where getWhereWithVersion() {
		return whereWithVersion;
	}
}
