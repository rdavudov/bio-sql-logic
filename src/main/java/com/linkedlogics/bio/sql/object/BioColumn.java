package com.linkedlogics.bio.sql.object;

import com.linkedlogics.bio.BioExpression;
import com.linkedlogics.bio.dictionary.BioTag;

/**
 * Represents a column in bio table
 * @author rdavudov
 *
 */
public class BioColumn {
    private BioTag tag;
    private String column;
    private int sqlType;
    private Object value ;
    
    private boolean isKey;
    private boolean isVersion;
    private boolean isBlob;
    private boolean isClob ;
    private boolean isXml ;
    private boolean isJson ;
    private boolean isAuto ;
    private boolean isHex ;
    private boolean isCompressed ;
    private boolean isEncrypted ;
    private boolean isEnumAsString ;
    private boolean isArray ;
    private boolean isList ;
    private boolean isMandatory ;
	
    public BioColumn(String name, int sqlType) {
    	this.column = name ;
    	this.sqlType = sqlType ;
    }
    
    public BioColumn(BioTag tag) {
    	this.tag = tag ;
    	this.isArray = tag.isArray() ;
    	this.isList = tag.isList() ;
    	this.isMandatory = tag.isMandatory() ;
    }
    
    /**
     * Returns tag information
     * @return
     */
    public BioTag getTag() {
		return tag;
	}
    /**
     * Sets tag information
     * @param tag
     */
	public void setTag(BioTag tag) {
		this.tag = tag;
	}
	
	/**
	 * Returns tag to put into bio object
	 * @return
	 */
	public String getTagName() {
		if (tag != null) {
			return tag.getName() ;
		}
		return column ;
	}
	/**
	 * Returns column name
	 * @return
	 */
	public String getColumn() {
		return column;
	}
	/**
	 * Sets column name
	 * @param column
	 */
	public void setColumn(String column) {
		this.column = column;
	}
	/**
	 * Returns SQL TYPE
	 * @return
	 */
	public int getSqlType() {
		return sqlType;
	}
	/**
	 * Sets SQL TYPE
	 * @param sqlType
	 */
	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}
	/**
	 * Indicates if column is Primary Key
	 * @return
	 */
	public boolean isKey() {
		return isKey;
	}
	/**
	 * Sets column as Primary Key
	 * @param isKey
	 */
	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}
	/**
	 * Indicates whether operations should consider VERSION value
	 * @return
	 */
	public boolean isVersion() {
		return isVersion;
	}
	/**
	 * Enables VERSION check
	 * @param isVersion
	 */
	public void setVersion(boolean isVersion) {
		this.isVersion = isVersion;
	}
	/**
	 * Indicates column type as BLOB
	 * @return
	 */
	public boolean isBlob() {
		return isBlob;
	}
	/**
	 * Sets column type as BLOB
	 * @param isBlob
	 */
	public void setBlob(boolean isBlob) {
		this.isBlob = isBlob;
	}
	/**
	 * Indicates column type as CLOB
	 * @return
	 */
	public boolean isClob() {
		return isClob;
	}
	/**
	 * Sets column type as CLOB
	 * @param isClob
	 */
	public void setClob(boolean isClob) {
		this.isClob = isClob;
	}
	/**
	 * Indicates value will be stored as XML
	 * @return
	 */
	public boolean isXml() {
		return isXml;
	}
	/**
	 * Sets column type to XML
	 * @param isXml
	 */
	public void setXml(boolean isXml) {
		this.isXml = isXml;
	}
	/**
	 * Indicates value will be stored as JSON
	 * @return
	 */
	public boolean isJson() {
		return isJson;
	}
	/**
	 * Sets column type to JSON
	 * @param isJson
	 */
	public void setJson(boolean isJson) {
		this.isJson = isJson;
	}
	/**
	 * Indicates value will be stored as binary HEX
	 * @return
	 */
	public boolean isHex() {
		return isHex;
	}
	/**
	 * Sets column type to HEX
	 * @param isHex
	 */
	public void setHex(boolean isHex) {
		this.isHex = isHex;
	}
	/**
	 * Indicates to compress HEX bytes
	 * @return
	 */
	public boolean isCompressed() {
		return isCompressed;
	}
	/**
	 * Sets compression enabled for HEX
	 * @param isCompressed
	 */
	public void setCompressed(boolean isCompressed) {
		this.isCompressed = isCompressed;
	}
	/**
	 * Indicates to encrypt HEX bytes
	 * @return
	 */
	public boolean isEncrypted() {
		return isEncrypted;
	}
	/**
	 * Set encryption enabled
	 * @param isEncrypted
	 */
	public void setEncrypted(boolean isEncrypted) {
		this.isEncrypted = isEncrypted;
	}
	/**
	 * Indicates store enum values as Strings instead of integers
	 * @return
	 */
	public boolean isEnumAsString() {
		return isEnumAsString;
	}
	/**
	 * Sets enum as string flag
	 * @param isEnumAsString
	 */
	public void setEnumAsString(boolean isEnumAsString) {
		this.isEnumAsString = isEnumAsString;
	}
	/**
	 * Indicates to use bio expression result as a value
	 * @return
	 */
	public Object getValue() {
		return value;
	}
	/**
	 * Sets bio expression
	 * @param expression
	 */
	public void setValue(Object value) {
		this.value = value;
	}
	/**
	 * Indicates values are arrays
	 * @return
	 */
	public boolean isArray() {
		return isArray;
	}
	/**
	 * Sets is array flag
	 * @param isArray
	 */
	public void setArray(boolean isArray) {
		this.isArray = isArray;
	}
	/**
	 * Indicates values are list
	 * @return
	 */
	public boolean isList() {
		return isList;
	}
	/**
	 * Sets is list flag
	 * @param isList
	 */
	public void setList(boolean isList) {
		this.isList = isList;
	}
	/**
	 * Indicates value is mandatory
	 * @return
	 */
	public boolean isMandatory() {
		return isMandatory;
	}
	/**
	 * Sets mandatory flag
	 * @param isMandatory
	 */
	public void setMandatory(boolean isMandatory) {
		this.isMandatory = isMandatory;
	}
	
}
