package com.linkedlogics.bio.sql.utility;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.linkedlogics.bio.sql.BioSqlDictionary;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.object.BioRelation;
import com.linkedlogics.bio.sql.object.BioTable;
import com.linkedlogics.bio.utility.StringUtility;

/**
 * Bio sql dictionary XML utilities
 * @author rajab
 *
 */
public class DictionaryUtility {
	/**
	 * Export dictionary to xml
	 * @param dictionary
	 * @return
	 */
	public static String toXml(BioSqlDictionary dictionary) {
		StringBuilder xml = new StringBuilder() ;
		xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n") ;
		xml.append("<sql-dictionary code=\"").append(dictionary.getCode()).append("\">\n") ;
		
		ArrayList<BioTable> list = new ArrayList<BioTable>() ;
		for(Entry<Integer, BioTable> e : dictionary.getCodeMap().entrySet()) {
			list.add(e.getValue()) ;
		}
		Collections.sort(list, new Comparator<BioTable>() {
			@Override
			public int compare(BioTable o1, BioTable o2) {
				return o1.getCode() - o2.getCode();
			}
		});
		for (BioTable t : list) {
			tableToXml(t, xml);
		}
		
		xml.append("</sql-dictionary>") ;
		return xml.toString() ;
	}
	/**
	 * Exports table to xml
	 * @param table
	 * @param xml
	 */
	private static void tableToXml(BioTable table, StringBuilder xml) {
		xml.append("\t<table code=\"").append(table.getCode()).append("\"") ;
		if (table.getSchema() != null) {
			xml.append(" schema=\"").append(table.getSchema()).append("\"") ;
		}
		xml.append(" name=\"").append(table.getTable()).append("\"") ;
		if (table.getObj() != null) {
			xml.append(" type=\"").append(table.getObj().getType()).append("\"") ;
		}
		xml.append(">\n") ;
		
		ArrayList<BioColumn> columnList = new ArrayList<BioColumn>() ;
		for (int i = 0; i < table.getColumns().length; i++) {
			columnList.add(table.getColumns()[i]) ;
		}
		Collections.sort(columnList, new Comparator<BioColumn>() {
			@Override
			public int compare(BioColumn o1, BioColumn o2) {
				return o1.getColumn().compareTo(o2.getColumn());
			}
		});
		for (BioColumn c : columnList) {
			columnToXml(c, xml);
		}
		
		for (BioRelation r : table.getRelations()) {
			relationToXml(r, xml);
		}
		
		xml.append("\t</table>\n") ;
	}
	/**
	 * Exports column to xml
	 * @param column
	 * @param xml
	 */
    private static void columnToXml(BioColumn column, StringBuilder xml) {
    	xml.append("\t\t<column") ;
		
    	if (column.getTag() != null) {
    		if (!column.getColumn().equalsIgnoreCase(column.getTag().getName())) {
    			xml.append(" name=\"").append(column.getColumn()).append("\"") ;
    		}
    		xml.append(" tag-name=\"").append(column.getTag().getName()).append("\"") ;
			xml.append(" tag-code=\"").append(column.getTag().getCode()).append("\"") ;
		} else {
			xml.append(" name=\"").append(column.getColumn()).append("\"") ;
		}
    	
    	xml.append(" type=\"").append(getSqlType(column.getSqlType())).append("\"") ;
		
		if (column.isArray()) {
			xml.append(" is-array=\"true\"") ;
		} else if (column.isList()) {
			xml.append(" is-list=\"true\"") ;
		}
		if (column.isMandatory()) {
			xml.append(" is-mandatory=\"true\"") ;
		}
		if (column.isKey()) {
			xml.append(" is-key=\"true\"") ;
		}
		if (column.isVersion()) {
			xml.append(" is-version=\"true\"") ;
		}
		if (column.isBlob()) {
			xml.append(" is-blob=\"true\"") ;
		}
		if (column.isClob()) {
			xml.append(" is-clob=\"true\"") ;
		}
		if (column.isXml()) {
			xml.append(" is-xml=\"true\"") ;
		} else if (column.isJson()) {
			xml.append(" is-json=\"true\"") ;
		} else if (column.isHex()) {
			xml.append(" is-hex=\"true\"") ;
			if (column.isCompressed()) {
				xml.append(" is-compressed=\"true\"") ;
			}
			if (column.isEncrypted()) {
				xml.append(" is-encrypted=\"true\"") ;
			}
		}
		
		if (column.isEnumAsString()) {
			xml.append(" is-enum-as-string=\"true\"") ;
		}
		
		if (column.getTag() == null) {
			xml.append(">\n") ;
			xml.append("\t\t\t<value type=\"Dynamic\">").append(column.getValue().toString()).append("</value>\n") ;
			xml.append("\t\t</column>\n") ;
		} else {
			xml.append("/>\n") ;
		}
    }
    /**
     * Exports relation to xml
     * @param relation
     * @param xml
     */
    private static void relationToXml(BioRelation relation, StringBuilder xml) {
    	xml.append("\t\t<relation") ;
    	
    	if (relation.getTag() != null) {
    		xml.append(" tag-name=\"").append(relation.getTag().getName()).append("\"") ;
			xml.append(" tag-code=\"").append(relation.getTag().getCode()).append("\"") ;
		}
    	xml.append(" type=\"").append(relation.getTag().getObj().getType()).append("\"") ;
    	
    	if (relation.getRelateKeys().length == 1) {
    		xml.append(" relate-key=\"") ;
    		xml.append(relation.getRelateKeys()[0]) ;
    		xml.append("\"") ;
    	} else {
    		xml.append(" relate-keys=\"") ;
    		xml.append(StringUtility.join(relation.getRelateKeys())) ;
    		xml.append("\"") ;
    	}
    	
    	if (relation.getToKeys().length == 1) {
    		xml.append(" to-key=\"") ;
    		xml.append(relation.getToKeys()[0]) ;
    		xml.append("\"") ;
    	} else {
    		xml.append(" to-keys=\"") ;
    		xml.append(StringUtility.join(relation.getToKeys())) ;
    		xml.append("\"") ;
    	}
		
		xml.append("/>\n") ;
    }
    /**
     * Returns string representation of a sql type
     * @param sqlType
     * @return
     */
    public static String getSqlType(int sqlType) {
    	Class c = Types.class;
		Field[] fields = c.getDeclaredFields() ;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getType() == int.class) {
				try {
					int type = fields[i].getInt(null) ;
					if (type == sqlType) {
						return fields[i].getName() ;
					}
				} catch (Throwable e) {

				}
			}
		}
		return "UNKNOWN" ;
    }
    
    public static int getSqlType(String sqlType) {
    	Class c = Types.class;
		Field[] fields = c.getDeclaredFields() ;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getType() == int.class) {
				try {
					int type = fields[i].getInt(null) ;
					if (fields[i].getName().equals(sqlType)) {
						return fields[i].getInt(null) ;
					}
				} catch (Throwable e) {

				}
			}
		}
		return -1 ;
    }
}
