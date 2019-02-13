package com.linkedlogics.bio.sql.object;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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
	private List<BioRelation> relations = new ArrayList<BioRelation>() ;
	
	public BioTable(int dictionary, int code) {
		this(BioDictionary.getDictionary(dictionary).getObjByCode(code), null, null) ;
	}
	
	public BioTable(int dictionary, int code, String table) {
		this(BioDictionary.getDictionary(dictionary).getObjByCode(code), table, null) ;
	}
	
	public BioTable(int dictionary, int code, String table, String schema) {
		this(BioDictionary.getDictionary(dictionary).getObjByCode(code), table, schema) ;
	}
	
	public BioTable(BioObj obj, String table, String schema) {
		this.obj = obj ;
		this.schema = schema  ;
		this.table = table ;
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
		this.columnByTagMap.put(column.getTag().getName(), column) ;
	}
	
	public void addRelation(BioRelation relation) {
		this.relations.add(relation) ;
	}
	
	public List<BioRelation> getRelations() {
		return relations;
	}

	public void generate() {
		this.columnByTagMap.entrySet().stream().forEach(e -> {
			this.columnByNameMap.put(e.getValue().getColumn(), e.getValue()) ;
		});
		this.columns = this.columnByTagMap.entrySet().stream().map(e -> {
			return e.getValue() ;
		}).toArray(size -> new BioColumn[size]) ;
		
		this.keys = (BioColumn[]) Arrays.stream(columns).filter(c -> {
			return c.isKey() ;
		}).toArray(size -> new BioColumn[size]) ;
		this.versionColumn = (BioColumn) Arrays.stream(columns).filter(c -> {
			return c.isVersion() ;
		}).findFirst().orElse(null) ;
		
		this.insert = SqlUtility.generateInsert(this) ;
		this.update = SqlUtility.generateUpdate(this) ;
		this.delete = SqlUtility.generateDelete(this) ;
		this.select = SqlUtility.generateSelect(this) ;
		this.count = SqlUtility.generateCount(this) ;
		this.where = SqlUtility.generateWhere(this) ;
		this.whereWithVersion = SqlUtility.generateWhereWithVersion(this) ;
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
