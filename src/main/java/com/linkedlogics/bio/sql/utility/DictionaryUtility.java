package com.linkedlogics.bio.sql.utility;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

import com.linkedlogics.bio.sql.BioSqlDictionary;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.object.BioRelation;
import com.linkedlogics.bio.sql.object.BioTable;

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
		
		dictionary.getCodeMap().entrySet().stream().map(e -> {
			return e.getValue() ;
		}).sorted(Comparator.comparing(BioTable::getCode)).collect(Collectors.toList()).forEach(o -> {
			tableToXml(o, xml);
		});
		
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
		
		Arrays.stream(table.getColumns()).sorted(Comparator.comparing(BioColumn::getColumn)).forEach(c -> {
			columnToXml(c, xml);
		});
		
		table.getRelations().forEach(r -> {
			relationToXml(r, xml);
		});
		
		xml.append("\t</table>\n") ;
	}
	/**
	 * Exports column to xml
	 * @param column
	 * @param xml
	 */
    private static void columnToXml(BioColumn column, StringBuilder xml) {
    	xml.append("\t\t<column") ;
    	xml.append(" name=\"").append(column.getColumn()).append("\"") ;
		
    	xml.append(" type=\"").append(getSqlType(column.getSqlType())).append("\"") ;
    	
    	if (column.getTag() != null) {
			xml.append(" tag=\"").append(column.getTag().getCode()).append("\"") ;
		}
		
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
		
		xml.append("/>\n") ;
    }
    /**
     * Exports relation to xml
     * @param relation
     * @param xml
     */
    private static void relationToXml(BioRelation relation, StringBuilder xml) {
    	xml.append("\t\t<relation") ;
    	xml.append(" type=\"").append(relation.getTag().getObj().getType()).append("\"") ;
    	
    	if (relation.getTag() != null) {
			xml.append(" tag-code=\"").append(relation.getTag().getCode()).append("\"") ;
			xml.append(" tag-name=\"").append(relation.getTag().getName()).append("\"") ;
		}
    	
    	if (relation.getRelateKeys().length == 1) {
    		xml.append(" relate-key=\"") ;
    		xml.append(relation.getRelateKeys()[0]) ;
    		xml.append("\"") ;
    	} else {
    		xml.append(" relate-keys=\"") ;
    		xml.append(Arrays.stream(relation.getRelateKeys()).collect(Collectors.joining(","))) ;
    		xml.append("\"") ;
    	}
    	
    	if (relation.getToKeys().length == 1) {
    		xml.append(" to-key=\"") ;
    		xml.append(relation.getToKeys()[0]) ;
    		xml.append("\"") ;
    	} else {
    		xml.append(" to-keys=\"") ;
    		xml.append(Arrays.stream(relation.getToKeys()).collect(Collectors.joining(","))) ;
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
}
