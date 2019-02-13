package com.linkedlogics.bio.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.linkedlogics.bio.BioExpression;
import com.linkedlogics.bio.BioObject;
import com.linkedlogics.bio.parser.BioObjectBinaryParser;
import com.linkedlogics.bio.parser.BioObjectXmlParser;
import com.linkedlogics.bio.sql.exception.SqlException;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.utility.SqlUtility;

/**
 * Provides basic SQL functionality over bio objects
 * @author rajab
 *
 * @param <T>
 */
public class BioSql<T extends BioObject> {
	/**
	 * database connection
	 */
	private Connection connection ;
	/**
	 * table object to work on
	 */
	private BioTable table ;
	/**
	 * if lazy doesn't consider relations
	 */
	private boolean isLazy ;
	/**
	 * connection auto commit flag
	 */
	private boolean isAutoCommit ;
	/**
	 * parser for parsing HEX values
	 */
	private BioObjectBinaryParser binaryParser ;
	/**
	 * parser for parsin XML values
	 */
	private BioObjectXmlParser xmlParser ;

	public BioSql(BioTable table) {
		this.table = table;
		this.binaryParser = new BioObjectBinaryParser() ;
		this.xmlParser = new BioObjectXmlParser() ;
		this.isLazy = true ;
	}

	/**
	 * Returns database connection
	 * @return
	 */
	public Connection getConnection() {
		return connection;
	}
	/**
	 * Sets database connection and checks whether auto commit is false
	 * @param connection
	 */
	public void setConnection(Connection connection) {
		this.connection = connection;
		try {
			isAutoCommit = connection.getAutoCommit() ;
		} catch (SQLException e) {
			throw new SqlException(e) ;
		}
	}
	/**
	 * Set auto commit off
	 */
	private void setAutoCommitOff() {
		// if auto commit is TRUE and is it lazy then we disable it
		if (isAutoCommit && !isLazy) {
			try {
				connection.setAutoCommit(false);
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		}
	}
	/**
	 * Set auto commit on
	 * @param isCommit
	 */
	private void setAutoCommitOn(boolean isCommit) {
		// if connection was auto commit and we were lazy, we set auto commit back and commit or rollback
		if (isAutoCommit && !isLazy) {
			try {
				if (isCommit) {
					connection.commit();
				} else {
					connection.rollback();
				}
				connection.setAutoCommit(true);
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		}
	}
	/**
	 * Indicates lazyness, it true then we also consider relations during sqls
	 * @return
	 */
	public boolean isLazy() {
		return isLazy;
	}
	/**
	 * Sets lazyness, it true then we also consider relations during sqls
	 * @param isLazy
	 */
	public void setLazy(boolean isLazy) {
		this.isLazy = isLazy;
	}
	/**
	 * Returns bio table
	 * @return
	 */
	public BioTable getTable() {
		return table;
	}
	/**
	 * Returns binary parser
	 * @return
	 */
	public BioObjectBinaryParser getBinaryParser() {
		return binaryParser;
	}
	/**
	 * Returns xml parser
	 * @return
	 */
	public BioObjectXmlParser getXmlParser() {
		return xmlParser;
	}
	/**
	 * Selects bio object by single primary key, if there are more than one PKs then you have to use select(BioObject objec)
	 * @param key
	 * @return
	 * @throws SQLException
	 */
	public T select(Object key) throws SQLException {
		if (table.getKeys() == null || table.getKeys().length == 0) {
			throw new SqlException(table.getTable() + " has no primary key columns use select() method") ;
		}
		if (table.getKeys().length > 1) {
			throw new SqlException(table.getTable() + " has more than one primary key columns use select() method") ;
		}

		String sql = getSql(table.getSelect(), table.getWhere(), null) ;
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			SqlUtility.setParameter(ps, 1, table.getWhere().getType(1), key);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					T newObject = (T) create() ;
					int index = 0;
					for (int i = 0; i < table.getColumns().length; i++) {
						index = index + 1;
						Object value = SqlUtility.getParameter(rs, index, table.getColumns()[i], binaryParser, xmlParser);
						if (value != null) {
							newObject.put(table.getColumns()[i].getTag().getName(), value);
						}
					}

					return newObject ;
				}
			}
		} catch (Throwable e) {
			throw new SqlException(e) ;
		}
		return null ;
	}
	/**
	 * Selects full table
	 * @return
	 * @throws SQLException
	 */
	public List<T> select() throws SQLException {
		return select(null, null);
	}
	/**
	 * Selects based on values inside bio object as a condition
	 * @param object
	 * @return
	 * @throws SQLException
	 */
	public List<T> select(BioObject object) throws SQLException {
		return select(null, new Where(object, table), null);
	}
	/**
	 * Selects based on condition provided inside where
	 * @param object
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public List<T> select(BioObject object, Where where) throws SQLException {
		return select(object, where, null);
	}
	/**
	 * Selects based on condition where and orders
	 * @param object
	 * @param where
	 * @param order
	 * @return
	 * @throws SQLException
	 */
	public List<T> select(BioObject object, Where where, Order order) throws SQLException {
		String sql = getSql(table.getSelect(), where, order);

		List<T> list = new LinkedList<T>();

		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			setWhereParameters(object, where, ps, 0) ;

			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					T newObject = (T) create() ;
					int index = 0;
					for (int i = 0; i < table.getColumns().length; i++) {
						index = index + 1;
						Object value = SqlUtility.getParameter(rs, index, table.getColumns()[i], binaryParser, xmlParser);
						if (value != null) {
							newObject.put(table.getColumns()[i].getTag().getName(), value);
						}
					}
					list.add(newObject);
				}
			}
		} catch (Throwable e) {
			throw new SqlException(e) ;
		}

		return list ;
	}
	/**
	 * Iterates all objects
	 * @return
	 * @throws SQLException
	 */
	public BioCursor<T> iterate() throws SQLException {
		return iterate(null, null);
	}
	/**
	 * Iterates based on condition provided inside bio object
	 * @param object
	 * @return
	 * @throws SQLException
	 */
	public BioCursor<T> iterate(BioObject object) throws SQLException {
		return iterate(null, new Where(object, table), null);
	}
	/**
	 * Iterates based on condition where
	 * @param object
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public BioCursor<T> iterate(BioObject object, Where where) throws SQLException {
		return iterate(object, where, null);
	}
	/**
	 * Iterates based on condition where and orders
	 * @param object
	 * @param where
	 * @param order
	 * @return
	 * @throws SQLException
	 */
	public BioCursor<T> iterate(BioObject object, Where where, Order order) throws SQLException {
		String sql = getSql(table.getSelect(), where, order);

		PreparedStatement ps = connection.prepareStatement(sql)  ;
		try {
			setWhereParameters(object, where, ps, 0) ;

			return new BioCursor<T>(this, ps.executeQuery(), ps) ;
		} catch (Throwable e) {
			throw new SqlException(e) ;
		}
	}
	/**
	 * Inserts bio object
	 * @param object
	 * @return
	 * @throws SQLException
	 */
	public int insert(T object) throws SQLException {
		setAutoCommitOff();
		try (PreparedStatement ps = connection.prepareStatement(table.getInsert())) {
            for (int i = 0; i < table.getColumns().length; i++) {
            	BioColumn column = table.getColumns()[i] ;
            	Object value = object.get(column.getTag().getName()) ;
            	if (value instanceof BioExpression) {
                	value = ((BioExpression) value).getValue(object) ;
                }
            	if (value != null) {
            		SqlUtility.setParameter(ps, i + 1, column.getSqlType(), value);
            	} else {
            		SqlUtility.setNull(ps, i + 1, column.getSqlType());
            	}
            }
            
            int result = ps.executeUpdate();
            setAutoCommitOn(true);
            return result ;
		} catch (SQLException e) {
        	setAutoCommitOn(false);
            throw e;
        } catch (Throwable e) {
        	setAutoCommitOn(false);
            throw new SQLException(e);
        } 
	}
	/**
	 * Updates bio object using PKs and version (if table contains)
	 * @param object
	 * @return
	 * @throws SQLException
	 */
	public int update(T object) throws SQLException {
		return update(object, table.getWhereWithVersion()) ;
	}
	/**
	 * Updates bio objects with provided values and provided condition
	 * @param object
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public int update(BioObject object, Where where) throws SQLException {
		setAutoCommitOff();
		String sql = getSql(table.getUpdate(), where, null);
		try (PreparedStatement ps = connection.prepareStatement(sql) ;) {
			for (int i = 0; i < table.getColumns().length; i++) {
				BioColumn column = table.getColumns()[i] ;
				Object value = object.get(column.getTag().getName()) ;
				if (value instanceof BioExpression) {
                	value = ((BioExpression) value).getValue(object) ;
                }
				if (value != null) {
					SqlUtility.setParameter(ps, i + 1, column.getSqlType(), value);
				} else {
					SqlUtility.setNull(ps, i + 1, column.getSqlType());
				}
			}
			setWhereParameters(object, where, ps, table.getColumns().length) ;
			int result = ps.executeUpdate();
            setAutoCommitOn(true);
            return result ;
		} catch (Throwable e) {
			setAutoCommitOff(); 
			throw e ;
		}
	}
	/**
	 * Merges bio object
	 * @param object
	 * @return
	 * @throws SQLException
	 */
	public int merge(T object) throws SQLException {
		return merge(object, table.getWhereWithVersion()) ;
	}
	/**
	 * Merges bio objects based on condition
	 * @param object
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public int merge(BioObject object, Where where) throws SQLException {
		setAutoCommitOff();
		String sql = getSql(SqlUtility.generateUpdate(table, object) , where, null);
		try (PreparedStatement ps = connection.prepareStatement(sql) ;) {
			int index = 0 ;
			for (int i = 0; i < table.getColumns().length; i++) {
				BioColumn column = table.getColumns()[i] ;
				if (object.has(column.getTag().getName())) {
					index++ ;
					Object value = object.get(column.getTag().getName()) ;
					if (value instanceof BioExpression) {
						value = ((BioExpression) value).getValue(object) ;
					}
					if (value != null) {
						SqlUtility.setParameter(ps, index, column.getSqlType(), value);
					} else {
						SqlUtility.setNull(ps, index, column.getSqlType());
					}
				}
			}
			
			setWhereParameters(object, where, ps, index) ;
			int result = ps.executeUpdate();
            setAutoCommitOn(true);
            return result ;
		} catch (Throwable e) {
			setAutoCommitOff(); 
			throw e ;
		}
	}
	/**
	 * Deletes bio object
	 * @param object
	 * @return
	 * @throws SQLException
	 */
	public int delete(T object) throws SQLException {
		return delete(object, table.getWhere()) ;
	}
	/**
	 * Deletes bio objects based on condition
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public int delete(Where where) throws SQLException {
		return delete(null, table.getWhere()) ;
	}
	/**
	 * Deletes bio objects based on condition
	 * @param object
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	private int delete(BioObject object, Where where) throws SQLException {
		setAutoCommitOff();
		String sql = getSql(table.getDelete(), where, null);
		try (PreparedStatement ps = connection.prepareStatement(sql) ;) {
			setWhereParameters(object, where, ps, 0) ;
			int result = ps.executeUpdate();
            setAutoCommitOn(true);
            return result ;
		} catch (Throwable e) {
			setAutoCommitOff(); 
			throw e ;
		}
	}

	/**
	 * Finalizes sql by adding where and order
	 * @param sql
	 * @param where
	 * @param order
	 * @return
	 */
	protected String getSql(String sql, Where where, Order order) {
		if (where != null && where.getWhere().length() > 0) {
			sql = sql + " where " + where ;
		}
		if (order != null) {
			sql = sql + " order by " + order.getOrder(table) ;
		}
		return sql;
	}
	
	/**
	 * Sets where parameters
	 * @param object
	 * @param where
	 * @param ps
	 * @param index
	 * @return
	 * @throws SQLException
	 */
    protected int setWhereParameters(BioObject object, Where where, PreparedStatement ps, int index) throws SQLException {
        if (where != null) {
        	HashMap<Integer, Object> valueMap = where.getValueMap();
            for (int i = 0; i < valueMap.size(); i++) {
                index = index + 1;
                Object value = valueMap.get(i + 1);
                
                if (value instanceof BioExpression) {
                	value = ((BioExpression) value).getValue(object) ;
                }
                
                if (value != null) {
                    SqlUtility.setParameter(ps, index, where.getType(i + 1), value);
                } else {
                	 SqlUtility.setNull(ps, index, where.getType(i + 1));
                }
            }
        }
        return index;
    }

    /**
     * Creates an empty bio object
     * @return
     */
	protected BioObject create() {
		Class bioClass = null;
		try {
			if (table.getObj() != null && table.getObj().getBioClass() != null) {
				bioClass = table.getObj().getBioClass();
				// we empty bio object because we don't want initials to be set, may be it is NULL in db
				return ((BioObject) bioClass.getConstructor().newInstance()).empty() ;
			} else {
				return new BioObject(0) ;
			}
		} catch (NoSuchMethodException e) {
			throw new SqlException(bioClass.getName() + " has no default constructor") ;
		} catch (Throwable e) {
			throw new SqlException(e) ;
		}
	}
}
