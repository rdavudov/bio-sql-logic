package com.linkedlogics.bio.sql.utility;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

import com.linkedlogics.bio.BioDictionary;
import com.linkedlogics.bio.BioEnum;
import com.linkedlogics.bio.BioObject;
import com.linkedlogics.bio.BioTime;
import com.linkedlogics.bio.dictionary.BioEnumObj;
import com.linkedlogics.bio.dictionary.BioObj;
import com.linkedlogics.bio.dictionary.BioType;
import com.linkedlogics.bio.exception.ParserException;
import com.linkedlogics.bio.expression.Dynamic;
import com.linkedlogics.bio.parser.BioObjectBinaryParser;
import com.linkedlogics.bio.parser.BioObjectXmlParser;
import com.linkedlogics.bio.sql.BioTable;
import com.linkedlogics.bio.sql.Where;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.utility.ByteUtility;
import com.linkedlogics.bio.utility.ConversionUtility;


public class SqlUtility {
	public static String generateInsert(BioTable table) {
		StringBuilder sql = new StringBuilder("insert into ") ;
		if (table.getSchema() != null && table.getSchema().trim().length() > 0) {
			sql.append(table.getSchema()).append(".") ;
		}
		sql.append(table.getTable()) ;
		sql.append(" (");
		sql.append(Arrays.stream(table.getColumns()).map(c -> {
			return c.getColumn() ;
		}).collect(Collectors.joining(",")));
		sql.append(") values(") ;
		sql.append(Arrays.stream(table.getColumns()).map(c -> {
			return "?" ;
		}).collect(Collectors.joining(",")));
		sql.append(")") ;
		
		return sql.toString() ;
	}
	
	public static String generateUpdate(BioTable table) {
		StringBuilder sql = new StringBuilder("update ") ;
		if (table.getSchema() != null && table.getSchema().trim().length() > 0) {
			sql.append(table.getSchema()).append(".") ;
		}
		sql.append(table.getTable()) ;
		sql.append(" set ");
		sql.append(Arrays.stream(table.getColumns()).map(c -> {
			return c.getColumn() + " = ?" ;
		}).collect(Collectors.joining(",")));
		
		return sql.toString() ;
	}
	
	public static String generateUpdate(BioTable table, BioObject object) {
		StringBuilder sql = new StringBuilder("update ") ;
		if (table.getSchema() != null && table.getSchema().trim().length() > 0) {
			sql.append(table.getSchema()).append(".") ;
		}
		sql.append(table.getTable()) ;
		sql.append(" set ");
		sql.append(Arrays.stream(table.getColumns()).map(c -> {
			if (object.has(c.getTag().getName()))
				return c.getColumn() + " = ?" ;
			return null ;
		}).filter(c -> {
			return c != null ;
		}).collect(Collectors.joining(",")));
		
		return sql.toString() ;
	}
	
	public static String generateSelect(BioTable table) {
		StringBuilder sql = new StringBuilder("select ") ;
		sql.append(Arrays.stream(table.getColumns()).map(c -> {
			return c.getColumn() ;
		}).collect(Collectors.joining(",")));
		sql.append(" from ") ;
		if (table.getSchema() != null && table.getSchema().trim().length() > 0) {
			sql.append(table.getSchema()).append(".") ;
		}
		sql.append(table.getTable()) ;
		
		return sql.toString() ;
	}
	
	public static String generateCount(BioTable table) {
		StringBuilder sql = new StringBuilder("select count(*) from ") ;
		if (table.getSchema() != null && table.getSchema().trim().length() > 0) {
			sql.append(table.getSchema()).append(".") ;
		}
		sql.append(table.getTable()) ;
		
		return sql.toString() ;
	}
	
	public static String generateDelete(BioTable table) {
		StringBuilder sql = new StringBuilder("delete from ") ;
		if (table.getSchema() != null && table.getSchema().trim().length() > 0) {
			sql.append(table.getSchema()).append(".") ;
		}
		sql.append(table.getTable()) ;
		
		return sql.toString() ;
	}
	
	public static Where generateWhere(BioTable table) {
		HashMap<Integer, Object> valueMap = new HashMap<Integer, Object>() ;
		HashMap<Integer, Integer> typeMap = new HashMap<Integer, Integer>() ;
		AtomicInteger index = new AtomicInteger(1) ;
		String where = Arrays.stream(table.getKeys()).map(c -> {
			valueMap.put(index.get(), new Dynamic(c.getTag().getName())) ;
			typeMap.put(index.get(), c.getSqlType()) ;
			index.getAndIncrement() ;
			return c.getColumn() + " = ?" ;
		}).collect(Collectors.joining(" and ")) ;
		
		return new Where(where, valueMap, typeMap) ;
	}
	
	public static Where generateWhereWithVersion(BioTable table) {
		HashMap<Integer, Object> valueMap = new HashMap<Integer, Object>() ;
		HashMap<Integer, Integer> typeMap = new HashMap<Integer, Integer>() ;
		AtomicInteger index = new AtomicInteger(1) ;
		String where = Arrays.stream(table.getKeys()).map(c -> {
			valueMap.put(index.get(), new Dynamic(c.getTag().getName())) ;
			typeMap.put(index.get(), c.getSqlType()) ;
			index.getAndIncrement() ;
			return c.getColumn() + " = ?" ;
		}).collect(Collectors.joining(" and ")) ;
		
		if (table.getVersionColumn() != null) {
			where = where + (" and (") + table.getVersionColumn().getColumn() + " < ? or -1 = ?)" ;
			valueMap.put(index.get(), new Dynamic(table.getVersionColumn().getTag().getName())) ;
			typeMap.put(index.get(), table.getVersionColumn().getSqlType()) ;
			valueMap.put(index.incrementAndGet() , new Dynamic(table.getVersionColumn().getTag().getName())) ;
			typeMap.put(index.get(), table.getVersionColumn().getSqlType()) ;
		}
		
		return new Where(where, valueMap, typeMap) ;
	}
	
	public static int getSqlType(Object value) {
		if (value instanceof Integer) {
			return Types.INTEGER ;
		} else if (value instanceof Long) {
			return Types.NUMERIC ;
		} else if (value instanceof Date) {
			return Types.TIMESTAMP ;
		} else if (value instanceof String) {
			return Types.VARCHAR ;
		} else if (value instanceof Boolean) {
			return Types.INTEGER ;
		} else if (value instanceof Double) {
			return Types.DOUBLE ;
		} else if (value instanceof BioObject) {
			return Types.VARCHAR ;
		} else if (value instanceof BioEnum) {
			return Types.INTEGER ;
		} else if (value instanceof Byte) {
			return Types.INTEGER ;
		}
		if (value == null) {
			return Types.VARCHAR ;
		} else {
			throw new RuntimeException("unknown sql type " + value.getClass().getName() + " " + value) ;
		}
	}
	
	public static int getSqlType(String type) {
		if ("Integer".equals(type)) {
			return Types.INTEGER ;
		} else if ("Long".equals(type)) {
			return Types.NUMERIC ;
		} else if ("Time".equals(type)) {
			return Types.TIMESTAMP ;
		} else if ("String".equals(type)) {
			return Types.VARCHAR ;
		} else if ("UtfString".equals(type)) {
			return Types.VARCHAR ;
		} else if ("Boolean".equals(type)) {
			return Types.INTEGER ;
		} else if ("Double".equals(type)) {
			return Types.DOUBLE ;
		} else if ("BioEnum".equals(type)) {
			return Types.INTEGER ;
		} else if ("Byte".equals(type)) {
			return Types.INTEGER ;
		} else if ("BioObject".equals(type)) {
			return Types.VARCHAR ;
		}
		throw new RuntimeException("unknown sql type") ;
	}
	
	public static int getSqlType(BioType type) {
		switch (type) {
		case Byte:
		case Short:
		case Integer:
		case BioEnum:
		case Boolean:
			return Types.INTEGER ;
		case Long:
			return Types.NUMERIC ;
		case Time:
			return Types.TIMESTAMP;
		case Float:
		case Double:
			return Types.DOUBLE ;
		case String:
		case UtfString:
		case BioObject:
		case Properties:
			return Types.VARCHAR ;
		}
		return -1 ;
	}
	
    public static void setParameter(PreparedStatement ps, int index, int sqlType, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(index, sqlType);
            return;
        }
        System.out.println(index + "=" + value);
        switch (sqlType) {
            case Types.VARCHAR:
            	if (value instanceof String) {
            		ps.setString(index, (String) value);
            	} else {
            		ps.setString(index, value.toString());
            	}
                break;
            case Types.INTEGER:
                if (value instanceof Number) {
                    ps.setInt(index, ((Number) value).intValue());
                } else if (value instanceof Boolean) {
                    ps.setInt(index, ((Boolean) value).booleanValue() ? 1 : 0);
                } else if (value instanceof BioEnum) {
                    ps.setInt(index, ((BioEnum) value).intValue());
                } else {
                    ps.setInt(index, Integer.parseInt(value.toString()));
                }
                break;
            case Types.NUMERIC:
                if (value instanceof Number) {
                    ps.setLong(index, ((Number) value).longValue());
                } else {
                    ps.setLong(index, Long.parseLong(value.toString()));
                }
                break;
            case Types.TIMESTAMP:
                if (value instanceof Number) {
                    ps.setTimestamp(index, new Timestamp(((Number) value).longValue()));
                } else if (value instanceof java.util.Date) {
                    ps.setTimestamp(index, new Timestamp(((java.util.Date) value).getTime()));
                } else {
                    ps.setTimestamp(index, new Timestamp(BioTime.parseString(value.toString())));
                }
                break;
            case Types.DATE:
                if (value instanceof Number) {
                    ps.setDate(index, new java.sql.Date(((Number) value).longValue()));
                } else if (value instanceof java.util.Date) {
                    ps.setDate(index, new java.sql.Date(((java.util.Date) value).getTime()));
                } else {
                    ps.setDate(index, new java.sql.Date(BioTime.parseString(value.toString())));
                }
                break;
            case Types.DOUBLE:
                if (value instanceof Number) {
                    ps.setDouble(index, ((Number) value).doubleValue());
                } else {
                    ps.setDouble(index, Double.parseDouble(value.toString()));
                }
                break;
            case Types.BLOB:
            	if (value instanceof Byte[]) {
            		 Byte[] bytes = (Byte[]) value;
                     byte[] array = new byte[bytes.length];
                     for (int i = 0; i < bytes.length; i++)
                         array[i] = bytes[i];
                     ps.setBlob(index, new ByteArrayInputStream(array));
            	} else {
                     byte[] array = (byte[]) value;
                     ps.setBlob(index, new ByteArrayInputStream(array));
            	}
                break;
            case Types.CLOB:
            	String string = (String) value ;
            	ps.setClob(index, new StringReader(string));
                break;
        }
    }
    
    public static void setNull(PreparedStatement ps, int index, int type) throws SQLException {
        ps.setNull(index, type);
    }
    
    public static Object getParameter(ResultSet rs, int index, BioColumn column, BioObjectBinaryParser binaryParser, BioObjectXmlParser xmlParser) throws SQLException {
    	if (column.isBlob()) {
    		 Blob blob = rs.getBlob(index);
             if (blob != null && blob.length() > 0) {
             	byte[] array = blob.getBytes(1, (int) blob.length());
             	
             	if (column.getTag().getType() == BioType.Byte) {
             		Byte[] bytes = new Byte[array.length];
                 	for (int i = 0; i < array.length; i++)
                 		bytes[i] = array[i];
                 	return bytes;
             	} else {
             		return binaryParser.decode(array) ;
             	}
             }
             return null ;
    	} else if (column.isJson()) {
    		String value = rs.getString(index);
            if (value != null) {
            	if (column.getTag().isArray() || column.getTag().isList()) {
            		JSONArray jsonArray = new JSONArray(value) ;
            		List<BioObject> list = new ArrayList<BioObject>() ;
            		BioObj obj = column.getTag().getObj() ;
            		if (obj != null) {
            			for (int i = 0; i < jsonArray.length(); i++) {
            				BioObject object = BioDictionary.getDictionary(obj.getDictionary()).getFactory().newBioObject(obj.getCode()) ;
            				object.putAll(BioObject.fromJson((JSONObject) jsonArray.get(i)));
            				list.add(object) ;
            			}
            		}
            		
            		if (column.getTag().isList()) {
            			return list ;
            		} else {
            			BioObject[] array = BioDictionary.getDictionary(obj.getDictionary()).getFactory().newBioObjectArray(obj.getCode(), list.size()) ;
            			for (int i = 0; i < array.length; i++) {
							array[i] = list.get(i) ;
						}
            			return array ;
            		}
        		} else {
        			JSONObject jsonObject = new JSONObject(value) ;
        			BioObj obj = column.getTag().getObj() ;
        			BioObject object = null ;
        			if (obj != null) {
        				object = BioDictionary.getDictionary(obj.getDictionary()).getFactory().newBioObject(obj.getCode()) ;
        			} else {
        				object = new BioObject(0) ;
        			}
    				object.putAll(BioObject.fromJson(jsonObject));
    				return object ;
        		}
            }
    	} else if (column.isXml()) {
    		String value = rs.getString(index);
            if (value != null) {
            	try {
					return xmlParser.parse(value) ;
				} catch (Exception e) {
					throw new ParserException(e) ;
				}
            }
    	} else if (column.isHex()) {
    		String value = rs.getString(index);
    		if (value != null) {
            	return binaryParser.decode(ByteUtility.hexToBytes(rs.getString(index)));
            }
    	} else if (column.isArray()) {
    		String value = rs.getString(index);
            if (value != null) {
            	if (column.getTag().getType() == BioType.BioEnum) {
            		String[] enums = (String[]) ConversionUtility.convertAsArray(column.getTag().getType(), value);
            		BioEnum[] enumArray = BioDictionary.getDictionary(column.getTag().getEnumObj().getDictionary()).getFactory().newBioEnumArray(column.getTag().getEnumObj().getCode(), enums.length) ;
            		for (int i = 0; i < enumArray.length; i++) {
            			if (column.isEnumAsString()) {
            				enumArray[i] = column.getTag().getEnumObj().getBioEnum(enums[i]) ;
            			} else {
            				enumArray[i] = column.getTag().getEnumObj().getBioEnum(Integer.parseInt(enums[i])) ;
            			}
					}
            		return enumArray ;
            	} else {
            		return (Object[]) ConversionUtility.convertAsArray(column.getTag().getType(), value);
            	}
            }
    	} else if (column.isList()) {
    		String value = rs.getString(index);
    		if (value != null) {
    			Object[] array = (Object[]) ConversionUtility.convertAsArray(column.getTag().getType(), value);
    			ArrayList<Object> list = new ArrayList<Object>();
    			if (column.getTag().getType() == BioType.BioEnum) {
    				for (int i = 0; i < array.length; i++) {
    					if (column.isEnumAsString()) {
    						list.add(column.getTag().getEnumObj().getBioEnum(array[i].toString())) ;
            			} else {
            				list.add(column.getTag().getEnumObj().getBioEnum(Integer.parseInt(array[i].toString()))) ;
            			}
    				}
    			} else {
    				for (int i = 0; i < array.length; i++) {
    					list.add(array[i]);
    				}
    				return list;
    			}
    		}
        } else {
            switch (column.getTag().getType()) {
                case Integer:
                    int intValue = rs.getInt(index);
                    return rs.wasNull() ? null : intValue;
                case Byte:
                    int byteValue = rs.getInt(index);
                    return rs.wasNull() ? null : (byte) byteValue;
                case Long:
                    long longValue = rs.getLong(index);
                    return rs.wasNull() ? null : longValue;
                case String:
                case UtfString:
                    return rs.getString(index);
                case Double:
                    double doubleValue = rs.getDouble(index);
                    return rs.wasNull() ? null : doubleValue;
                case Boolean:
                    int booleanValue = rs.getInt(index);
                    return rs.wasNull() ? null : booleanValue == 1;
                case Time:
                    Timestamp time = rs.getTimestamp(index);
                    if (time != null) {
                        return time.getTime();
                    }
                    return null;
                case BioEnum:
                    BioEnumObj enumObj = column.getTag().getEnumObj();
                    if (enumObj != null) {
                    	if (column.isEnumAsString()) {
                    		String enumName = rs.getString(index);
                            return rs.wasNull() ? null : enumObj.getBioEnum(enumName);
                    	} else {
                    		int enumCode = rs.getInt(index);
                            return rs.wasNull() ? null : enumObj.getBioEnum(enumCode);
                    	}
                    }
                    return null;
                case Properties:
                case BioObject:
                    if (rs.getString(index) != null) {
                    	return binaryParser.decode(ByteUtility.hexToBytes(rs.getString(index)));
                    }
                    return null;
            }
        }
        return null;
    }
}
