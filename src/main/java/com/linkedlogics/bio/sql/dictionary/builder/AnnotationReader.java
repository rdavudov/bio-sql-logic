package com.linkedlogics.bio.sql.dictionary.builder;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;

import com.linkedlogics.bio.BioDictionary;
import com.linkedlogics.bio.BioDictionaryBuilder;
import com.linkedlogics.bio.BioObject;
import com.linkedlogics.bio.annotation.BioPojo;
import com.linkedlogics.bio.annotation.BioRemoteObj;
import com.linkedlogics.bio.annotation.BioRemoteTag;
import com.linkedlogics.bio.dictionary.BioObj;
import com.linkedlogics.bio.dictionary.BioTag;
import com.linkedlogics.bio.dictionary.builder.DictionaryReader;
import com.linkedlogics.bio.exception.DictionaryException;
import com.linkedlogics.bio.expression.Dynamic;
import com.linkedlogics.bio.sql.BioSqlDictionary;
import com.linkedlogics.bio.sql.BioTable;
import com.linkedlogics.bio.sql.annotation.BioRemoteSqlTag;
import com.linkedlogics.bio.sql.annotation.BioRemoteSqlTags;
import com.linkedlogics.bio.sql.annotation.BioSql;
import com.linkedlogics.bio.sql.annotation.BioSqlTag;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.utility.SqlUtility;
import com.linkedlogics.bio.utility.POJOUtility;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

public class AnnotationReader implements DictionaryReader {
	private String packageName ;

	public AnnotationReader() {

	}
	
	public AnnotationReader(String packageName) {
		this.packageName = packageName ;
	}
	
	@Override
	public void read(BioDictionaryBuilder builder) {
		ClassGraph graph = new ClassGraph().enableAnnotationInfo()
				.enableClassInfo()
				.enableFieldInfo();
				
		if (packageName != null) {
			graph.whitelistPackages(packageName) ;
		}
		
		try (ScanResult scanResult = graph.scan()) {
			// Finding all bio objects
			for (ClassInfo classInfo : scanResult.getClassesWithAnnotation(com.linkedlogics.bio.sql.annotation.BioSql.class.getName())) {
				if (!checkProfile(classInfo.getName(), builder.getProfiles(), builder.isOnlyProfiles())) {
					continue ;
				}
				BioTable table = createTable(classInfo.getName());
				if (table != null) {
					BioSqlDictionary.getOrCreateDictionary(table.getObj().getDictionary()).addTable(table);
				}
			}
			
			
			for (ClassInfo classInfo : scanResult.getClassesWithAnnotation(com.linkedlogics.bio.annotation.BioRemoteObj.class.getName())) {
				if (!checkProfile(classInfo.getName(), builder.getProfiles(), builder.isOnlyProfiles())) {
					continue ;
				}

				Class bioRemoteClass = classInfo.loadClass();
				BioRemoteObj remoteAnnotation = (BioRemoteObj) bioRemoteClass.getAnnotation(BioRemoteObj.class) ;
				Field[] fields = bioRemoteClass.getDeclaredFields();
				for (int j = 0; j < fields.length; j++) {
					if (fields[j].isAnnotationPresent(BioRemoteSqlTag.class)) {
						try {
							BioRemoteSqlTag annotation = fields[j].getAnnotation(BioRemoteSqlTag.class);
							BioObj obj = BioDictionary.getOrCreateDictionary(remoteAnnotation.dictionary()).getObjByType(annotation.obj()) ;
							if (obj != null) {
								BioColumn column = createRemoteColumn(fields[j], obj);
								if (column != null) {
									BioTable table =  BioSqlDictionary.getOrCreateDictionary(obj.getDictionary()).getTableByCode(obj.getCode()) ;
									if (table != null) {
										table.addColumn(column);
									}
								}
							}
							
						} catch (Throwable e) {
							throw new DictionaryException(e) ;
						}
					} else if (fields[j].isAnnotationPresent(BioRemoteSqlTags.class)) {
						BioRemoteTag[] array = (BioRemoteTag[]) fields[j].getAnnotationsByType(BioRemoteTag.class) ;
						for (int k = 0; k < array.length; k++) {
							try {
								BioRemoteTag annotation = array[k] ;
								
								BioObj obj = BioDictionary.getOrCreateDictionary(remoteAnnotation.dictionary()).getObjByType(annotation.obj()) ;
								if (obj != null) {
									BioColumn column = createRemoteColumn(fields[j], obj);
									if (column != null) {
										BioTable table =  BioSqlDictionary.getOrCreateDictionary(obj.getDictionary()).getTableByCode(obj.getCode()) ;
										if (table != null) {
											table.addColumn(column);
										}
									}
								}
								
							} catch (Throwable e) {
								throw new DictionaryException(e) ;
							}
						}
					}
				}
			}
		}

		// Finding all remote tag contained objects
		
	}

	private BioTable createTable(String tableClassName) {
		try {
			Class bioClass = Class.forName(tableClassName) ;
			if (bioClass.getAnnotation(com.linkedlogics.bio.annotation.BioObj.class) == null 
					&& bioClass.getAnnotation(BioPojo.class) == null) {
				throw new DictionaryException("@BioSql must be used together with @BioObj or @BioPojo in class" + tableClassName) ;
			}
			int code = 0 ;
			int dictionary = 0 ;
			if (bioClass.getAnnotation(com.linkedlogics.bio.annotation.BioObj.class) != null) {
				code = ((com.linkedlogics.bio.annotation.BioObj) bioClass.getAnnotation(com.linkedlogics.bio.annotation.BioObj.class)).code() ;
				dictionary = ((com.linkedlogics.bio.annotation.BioObj) bioClass.getAnnotation(com.linkedlogics.bio.annotation.BioObj.class)).dictionary() ;
			} else {
				code = ((BioPojo) bioClass.getAnnotation(BioPojo.class)).code() ;
				dictionary = ((BioPojo) bioClass.getAnnotation(BioPojo.class)).dictionary() ;
				if (code == 0) {
					code = POJOUtility.getCode(tableClassName) ;
				}
			}
			
			BioObj obj = BioDictionary.getDictionary(dictionary).getObjByCode(code) ;
			if (obj == null) {
				throw new DictionaryException(code + " bio obj is not found in dictionary") ;
			}
			
			BioSql sqlAnnotation = (BioSql) bioClass.getAnnotation(BioSql.class) ;
			String tableName = sqlAnnotation.table() ;
			if (tableName == null || tableName.length() == 0) {
				tableName = obj.getName() ;
			}
			String schema = sqlAnnotation.schema();
			if (schema != null && schema.length() == 0) {
				schema = null ;
			}
			BioTable table = new BioTable(obj, sqlAnnotation.table(), sqlAnnotation.schema()) ;
		
			
			HashMap<String, BioColumn> columnMap = new HashMap<String, BioColumn>();
			while (bioClass != BioObject.class) {
				Arrays.stream(bioClass.getDeclaredFields()).filter(f -> {
					return f.isAnnotationPresent(BioSqlTag.class) ;
				}).forEach(f -> {
					try {
						BioColumn column = createColumn(f, obj) ;
						columnMap.putIfAbsent(column.getTag().getName(), column) ;
					} catch (Throwable e) {
						throw new DictionaryException(e) ;
					}
				}) ;
				
				bioClass = bioClass.getSuperclass();
			}
			
			columnMap.values().stream().forEach(c -> {
				table.addColumn(c);
			});
			
			return table ;
		} catch (Throwable e) {
			throw new DictionaryException(e) ;
		}
	}
	
	protected BioColumn createColumn(Field f, BioObj obj) {
		BioTag tag ; 
		try {
			tag = obj.getTag((String) f.get(null)) ;
		} catch (IllegalAccessException e) {
			throw new DictionaryException(e) ;
		}
		
		if (tag != null) {
			BioSqlTag tagAnnotation = (BioSqlTag) f.getAnnotation(BioSqlTag.class) ;
			
			return createColumn(tagAnnotation.column(), tagAnnotation.isBlob(), tagAnnotation.isClob(), tagAnnotation.isJson(), tagAnnotation.isXml(), 
					tagAnnotation.isHex(), tagAnnotation.isCompressed(), tagAnnotation.isEncrypted(), tagAnnotation.isKey(), tagAnnotation.isVersion(), tagAnnotation.isEnumAsString(), tag) ;
		} else {
			throw new DictionaryException("@BioSqlTag must be used together with @BioTag annotation in class " + obj.getBioClass()) ;
		}
	}
	
	protected BioColumn createRemoteColumn(Field f, BioObj obj) {
		BioTag tag ; 
		try {
			tag = obj.getTag((String) f.get(null)) ;
		} catch (IllegalAccessException e) {
			throw new DictionaryException(e) ;
		}
		
		if (tag != null) {
			BioRemoteSqlTag tagAnnotation = (BioRemoteSqlTag) f.getAnnotation(BioRemoteSqlTag.class) ;
			
			return createColumn(tagAnnotation.column(), tagAnnotation.isBlob(), tagAnnotation.isClob(), tagAnnotation.isJson(), tagAnnotation.isXml(), 
					tagAnnotation.isHex(), tagAnnotation.isCompressed(), tagAnnotation.isEncrypted(), tagAnnotation.isKey(), tagAnnotation.isVersion(), tagAnnotation.isEnumAsString(), tag) ;
		} else {
			throw new DictionaryException("@BioSqlTag must be used together with @BioTag annotation in class " + obj.getBioClass()) ;
		}
	}
	
	private BioColumn createColumn(String columnName, boolean isBlob, boolean isClob, boolean isJson, 
			boolean isXml, boolean isHex, boolean isCompressed, boolean isEncrypted, boolean isKey, boolean isVersion, boolean isEnumAsString, BioTag tag) {
		BioColumn column = new BioColumn(tag) ;
		column.setBlob(isBlob);
		column.setClob(isClob);
		column.setJson(isJson);
		column.setXml(isXml);
		column.setHex(isHex);
		column.setCompressed(isCompressed);
		column.setEncrypted(isEncrypted);
		column.setKey(isKey);
		column.setVersion(isVersion);
		column.setEnumAsString(isEnumAsString);
		column.setExpression(new Dynamic(tag.getName()));
		if (columnName != null && columnName.trim().length() > 0) {
			column.setColumn(columnName);
		} else {
			column.setColumn(tag.getName());
		}						
		if (isBlob) 
			column.setSqlType(Types.BLOB) ;
		else if (isClob)
			column.setSqlType(Types.CLOB) ;
		else 
			column.setSqlType(SqlUtility.getSqlType(tag.getType())) ;
		
		return column ;
	}
}
