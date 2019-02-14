package com.linkedlogics.bio.sql;

import java.lang.reflect.Array;
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
import com.linkedlogics.bio.sql.object.BioTable;
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
	protected Connection connection ;
	/**
	 * table object to work on
	 */
	protected BioTable table ;
	/**
	 * if lazy doesn't consider relations
	 */
	protected boolean isLazy ;
	/**
	 * connection auto commit flag
	 */
	protected boolean isAutoCommit ;
	/**
	 * parser for parsing HEX values
	 */
	protected BioObjectBinaryParser binaryParser ;
	/**
	 * parser for parsin XML values
	 */
	protected BioObjectXmlParser xmlParser ;

	/**
	 * Creates sql object with table according bio code
	 * @param code
	 */
	public BioSql(int code) {
		this(0, code) ;
	}
	
	/**
	 * Creates sql object with table according bio code and dictionary
	 * @param dictionary
	 * @param code
	 */
	public BioSql(int dictionary, int code) {
		this.table = BioSqlDictionary.getDictionary(dictionary).getTableByCode(code) ;
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

		T object = null ;
		
		String sql = getSql(table.getSelect(), table.getWhere(), null) ;
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			SqlUtility.setParameter(ps, 1, table.getWhere().getType(1), key);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					object = (T) create() ;
					int index = 0;
					for (int i = 0; i < table.getColumns().length; i++) {
						index = index + 1;
						Object value = SqlUtility.getParameter(rs, index, table.getColumns()[i], binaryParser, xmlParser);
						if (value != null) {
							object.put(table.getColumns()[i].getTag().getName(), value);
						}
					}
				}
			}
		} catch (Throwable e) {
			throw new SqlException(e) ;
		}
		
		// if it is NOT lazy then we try to load related bio objects also
		if (object != null && !isLazy && table.getRelations().size() > 0) {
			selectRelations(object);
		}
		
		return object ;
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
			SqlUtility.setWhereParameters(object, where, ps, 0) ;

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
		
		// if it is NOT lazy then we try to load related bio objects also
		if (list.size() > 0 && !isLazy && table.getRelations().size() > 0) {
			list.stream().forEach(o -> {
				try {
					selectRelations(o);
				} catch (SQLException e) {
					throw new SqlException(e) ;
				}
			});
		}
		
		return list ;
	}

	/**
	 * Returns count of object
	 * @param object
	 * @return
	 * @throws SQLException
	 */
	public int count(T object) throws SQLException {
		return count(object, table.getWhere());
	}
	/**
	 * Returns count of objects
	 * @param object
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public int count(BioObject object, Where where) throws SQLException {
		String sql = getSql(table.getCount(), where, null) ;
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			SqlUtility.setWhereParameters(object, where, ps, 0) ;
			try (ResultSet rs = ps.executeQuery()) {
	            if (rs.next()) {
	                return rs.getInt(1);
	            }
			}
		} catch (Throwable e) {
			throw new SqlException(e) ;
		}
		return 0 ;
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
			SqlUtility.setWhereParameters(object, where, ps, 0) ;

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
            	SqlUtility.setParameters(ps, i + 1, getValue(object, column), column, binaryParser, xmlParser) ;
            }
            int result = ps.executeUpdate();
            
            // if it is NOT lazy then we try to insert related bio objects also
            if (result > 0 && !isLazy && table.getRelations().size() > 0) {
				insertRelations((T) object) ;
			}
            
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
				SqlUtility.setParameters(ps, i + 1, getValue(object, column), column, binaryParser, xmlParser) ;
			}
			SqlUtility.setWhereParameters(object, where, ps, table.getColumns().length) ;
			int result = ps.executeUpdate();
			
            // if it is NOT lazy then we try to update related bio objects also
			if (result > 0 && !isLazy && table.getRelations().size() > 0) {
				updateRelations((T) object) ;
			}
			
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
					SqlUtility.setParameters(ps, index, getValue(object, column), column, binaryParser, xmlParser) ;
				}
			}
			SqlUtility.setWhereParameters(object, where, ps, index) ;
			int result = ps.executeUpdate();
            // if it is NOT lazy then we try to merge related bio objects also
			if (result > 0 && !isLazy && table.getRelations().size() > 0) {
				mergeRelations((T) object) ;
			}
			
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
	public int delete(BioObject object, Where where) throws SQLException {
		setAutoCommitOff();
		String sql = getSql(table.getDelete(), where, null);
		try (PreparedStatement ps = connection.prepareStatement(sql) ;) {
			SqlUtility.setWhereParameters(object, where, ps, 0) ;
			int result = ps.executeUpdate();
            setAutoCommitOn(true);
            // if it is NOT lazy then we try to delete related bio objects also
            if (result > 0 && !isLazy && table.getRelations().size() > 0) {
				deleteRelations((T) object) ;
			}
            
            return result ;
		} catch (Throwable e) {
			setAutoCommitOff(); 
			throw e ;
		}
	}
	
	/**
	 * Selects related bio objects
	 * @param object
	 * @throws SQLException
	 */
	void selectRelations(T object) throws SQLException {
		table.getRelations().stream().forEach(r -> {
			BioSql sql = new BioSql(r.getTag().getObj().getCode()) ;
			sql.setConnection(connection);
			sql.setLazy(isLazy);
			try {
				List<BioObject> list = sql.select(object, r.getWhere()) ;
				if (list.size() > 0) {
					// if bio tag is array or list then we create collection
					if (r.isMany()) {
						if (r.getTag().isArray()) {
							BioObject[] array = (BioObject[]) Array.newInstance(r.getTag().getObj().getBioClass(), list.size());
							list.toArray(array) ;
							object.set(r.getTag().getName(), array) ;
						} else {
							object.set(r.getTag().getName(), list) ;
						}
					// or we just pick the first one
					} else {
						object.set(r.getTag().getName(), list.get(0)) ;
					}
				}
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		});
	}
	
	/**
	 * Inserts related bio objects
	 * @param object
	 * @throws SQLException
	 */
	void insertRelations(T object) throws SQLException {
		table.getRelations().stream().forEach(r -> {
			BioSql sql = new BioSql(r.getTag().getObj().getCode()) ;
			sql.setConnection(connection);
			sql.setLazy(isLazy);
			try {
				// if objects contains related objects
				if (object.has(r.getTag().getName())) {
					if (r.isMany()) {
						if (r.getTag().isArray()) {
							BioObject[] array = (BioObject[]) object.get(r.getTag().getName()) ;
							for (int i = 0; i < array.length; i++) {
								sql.insert(array[i]) ;
							}
						} else {
							List<BioObject> list = (List<BioObject>) object.get(r.getTag().getName()) ;
							for (BioObject o : list) {
								sql.insert(o) ;
							}
						}
					} else {
						sql.insert(object.getBioObject(r.getTag().getName())) ;
					}
				}
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		});
	}
	/**
	 * Updates related bio objects
	 * @param object
	 * @throws SQLException
	 */
	void updateRelations(T object) throws SQLException {
		table.getRelations().stream().forEach(r -> {
			BioSql sql = new BioSql(r.getTag().getObj().getCode()) ;
			sql.setConnection(connection);
			sql.setLazy(isLazy);
			try {
				// first we delete all of them 
				sql.delete(object, r.getWhere()) ;
				// then if object contains insert current list
				if (object.has(r.getTag().getName())) {
					if (r.isMany()) {
						if (r.getTag().isArray()) {
							BioObject[] array = (BioObject[]) object.get(r.getTag().getName()) ;
							for (int i = 0; i < array.length; i++) {
								sql.insert(array[i]) ;
							}
						} else {
							List<BioObject> list = (List<BioObject>) object.get(r.getTag().getName()) ;
							for (BioObject o : list) {
								sql.insert(o) ;
							}
						}
					} else {
						sql.insert(object.getBioObject(r.getTag().getName())) ;
					}
				}
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		});
	}
	
	/**
	 * Merges related bio objects
	 * @param object
	 * @throws SQLException
	 */
	void mergeRelations(T object) throws SQLException {
		table.getRelations().stream().forEach(r -> {
			BioSql sql = new BioSql(r.getTag().getObj().getCode()) ;
			sql.setConnection(connection);
			sql.setLazy(isLazy);
			try {
				// if objects contains new list we do otherwise no change
				if (object.has(r.getTag().getName())) {
					if (r.isMany()) {
						// if it is a collection we only add new ones, and merge old ones we don't remove any of them
						if (r.getTag().isArray()) {
							BioObject[] array = (BioObject[]) object.get(r.getTag().getName()) ;
							for (int i = 0; i < array.length; i++) {
								if (sql.count(array[i]) == 0) {
									sql.insert(array[i]) ;
								} else {
									sql.merge(array[i]) ;
								}
							}
						} else {
							List<BioObject> list = (List<BioObject>) object.get(r.getTag().getName()) ;
							for (BioObject o : list) {
								if (sql.count(o) == 0) {
									sql.insert(o) ;
								} else {
									sql.merge(o) ;
								}
							}
						}
					} else {
						// if it is a single we chech for existence and insert or merge
						if (sql.count(object.getBioObject(r.getTag().getName())) == 0) {
							sql.insert(object.getBioObject(r.getTag().getName())) ;
						} else {
							sql.merge(object.getBioObject(r.getTag().getName())) ;
						}
					}
				}
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		});
	}
	/**
	 * Deletes related bio objects
	 * @param object
	 * @throws SQLException
	 */
	public void deleteRelations(T object) throws SQLException {
		table.getRelations().stream().forEach(r -> {
			BioSql sql = new BioSql(r.getTag().getObj().getCode()) ;
			sql.setConnection(connection);
			sql.setLazy(isLazy);
			try {
				// just delete all of them
				sql.delete(object, r.getWhere()) ;
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		});
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
	 * Finalizes sql by adding where
	 * @param sql
	 * @param where
	 * @return
	 */
	protected String getSql(String sql, Where where) {
		return getSql(sql, where, null) ;
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
	
	/**
	 * Each column has a value it is dynamic by default which gets a tag value from bio object
	 * but it also can be a constant value or another expression not related to bio object for example systime
	 * @param object
	 * @param column
	 * @return
	 */
	protected Object getValue(BioObject object, BioColumn column) {
		Object value = column.getValue() ;
		if (value instanceof BioExpression) {
			value = ((BioExpression) value).getValue(object) ;
		}
		return value ;
	}
}
