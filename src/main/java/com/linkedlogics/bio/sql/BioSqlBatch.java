package com.linkedlogics.bio.sql;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import com.linkedlogics.bio.BioExpression;
import com.linkedlogics.bio.BioObject;
import com.linkedlogics.bio.sql.exception.SqlException;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.utility.SqlUtility;

/**
 * BioSqlBatch is batch SQL processor
 * @author rajab
 *
 */
public class BioSqlBatch implements AutoCloseable {
	/**
	 * Stores objects to be inserted
	 */
	private ArrayList<BioObject> insert = new ArrayList<BioObject>() ;
	/**
	 * Stores objects to be updated
	 */
	private ArrayList<BioObject> update = new ArrayList<BioObject>() ;
	/**
	 * Stores objects to be deleted
	 */
	private ArrayList<BioObject> delete = new ArrayList<BioObject>() ;
	/**
	 * Stores objects successfully processed or returned > 0
	 */
	private ArrayList<BioObject> success = new ArrayList<BioObject>() ;
	/**
	 * Stores objects failed by where condition or returned 0
	 */
	private ArrayList<BioObject> failed = new ArrayList<BioObject>() ;
	/**
	 * Stores objects failed with exception
	 */
	private ArrayList<BioObject> error = new ArrayList<BioObject>() ;
	/**
	 * bath size 
	 */
	protected int batchSize ;
	/**
	 * commit interval
	 */
	protected int commitSize ;
	/**
	 * BioSql instance for SQL generation
	 */
	protected BioSql<BioObject> bioSql ;
	/**
	 * Commit counter
	 */
	protected int commitCounter ;
	/**
	 * Update where clause
	 */
	protected Where updateWhere ;
	/**
	 * Delete where clause
	 */
	protected Where deleteWhere ;
	
	public BioSqlBatch(int code) {
		this(0, code) ;
	}

	public BioSqlBatch(int dictionary, int code) {
		bioSql = new BioSql<BioObject>(dictionary, code) ;
		updateWhere = bioSql.getTable().getWhereWithVersion() ;
		deleteWhere = bioSql.getTable().getWhere() ;
	}
	/**
	 * Sets database connection
	 * @param connection
	 */
	public void setConnection(Connection connection) {
		bioSql.setConnection(connection);
		try {
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			throw new SqlException(e) ;
		}
	}
	/**
	 * Returns database connection
	 * @return
	 */
	public Connection getConnection() {
		return bioSql.getConnection() ;
	}
	/**
	 * Returns batch size
	 * @return
	 */
	public int getBatchSize() {
		return batchSize;
	}
	/**
	 * Sets batch size
	 * @param batchSize
	 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	/**
	 * Returns commit interval
	 * @return
	 */
	public int getCommitSize() {
		return commitSize;
	}
	/**
	 * Sets commit interval
	 * @param commitSize
	 */
	public void setCommitSize(int commitSize) {
		this.commitSize = commitSize;
	}
	/**
	 * Returns update where clause
	 * @return
	 */
	public Where getUpdateWhere() {
		return updateWhere;
	}
	/**
	 * Sets where clause merged with version check
	 * @param updateWhere
	 */
	public void setUpdateWhere(Where updateWhere) {
		this.updateWhere = updateWhere.merge(bioSql.getTable().getWhereWithVersion());
	}
	/**
	 * Returns delete where clause
	 * @return
	 */
	public Where getDeleteWhere() {
		return deleteWhere;
	}
	/**
	 * Sets where clause merged with PK check
	 * @param deleteWhere
	 */
	public void setDeleteWhere(Where deleteWhere) {
		this.deleteWhere = deleteWhere.merge(bioSql.getTable().getWhere());
	}
	/**
	 * Adds an insert batch
	 * @param object
	 * @throws SQLException
	 */
	public void insert(BioObject object) throws SQLException {
		if (insert.add(object)) {
			if (insert.size() >= batchSize) {
				execute(bioSql.getTable().getInsert(), insert, null); 
			}
		}
	}
	/**
	 * Adds an update batch
	 * @param object
	 * @throws SQLException
	 */
	public void update(BioObject object) throws SQLException {
		if (update.add(object)) {
			if (update.size() >= batchSize) {
				execute(bioSql.getSql(bioSql.getTable().getUpdate(), updateWhere), update, updateWhere); 
			}
		}
	}
	/**
	 * Adds a delete batch
	 * @param object
	 * @throws SQLException
	 */
	public void delete(BioObject object) throws SQLException {
		if (delete.add(object)) {
			if (delete.size() >= batchSize) {
				execute(bioSql.getSql(bioSql.getTable().getDelete(), deleteWhere), delete, deleteWhere); 
			}
		}
	}
	/**
	 * Executes batch
	 * @param sql
	 * @param list
	 * @param where
	 * @throws SQLException
	 */
	protected void execute(String sql, ArrayList<BioObject> list, Where where) throws SQLException {
		try(PreparedStatement ps = bioSql.getConnection().prepareStatement(sql)) {
			for (int i = 0; i < list.size(); i++) {
				setBatchParameters(list.get(i), ps, where);
			}
			int[] result = null ;
			try {
				result = ps.executeBatch() ;
			} catch (BatchUpdateException e) {
				result = e.getUpdateCounts() ;
			}

			for (int i = 0; i < result.length; i++) {
				if (result[i] > 0) {
					success.add(list.get(i)) ;
				} else if (result[i] == 0){
					failed.add(list.get(i)) ;
				} else {
					error.add(list.get(i)) ;
				}
			}
		}
		commitCounter += list.size() ;
		if (commitCounter >= commitSize) {
			bioSql.getConnection().commit();
			commitCounter = 0 ;
		}
		
		list.clear();
	}
	/**
	 * Flushes remaining objects
	 * @throws SQLException
	 */
	public void flush() throws SQLException {
		execute(bioSql.getTable().getInsert(), insert, null); 
		execute(bioSql.getTable().getUpdate(), update, updateWhere); 
		execute(bioSql.getTable().getDelete(), delete, deleteWhere); 
	}
	/**
	 * Sets batch parameters
	 * @param object
	 * @param ps
	 * @param where
	 * @throws SQLException
	 */
	private void setBatchParameters(BioObject object, PreparedStatement ps, Where where) throws SQLException {
		for (int i = 0; i < bioSql.getTable().getColumns().length; i++) {
			BioColumn column = bioSql.getTable().getColumns()[i] ;
			Object value = object.get(column.getTag().getName()) ;
			if (value instanceof BioExpression) {
				value = ((BioExpression) value).getValue(object) ;
			}
			
			SqlUtility.setParameters(ps, i + 1, value, column, bioSql.getBinaryParser(), bioSql.getXmlParser()) ;
		}
		SqlUtility.setWhereParameters(object, where, ps, bioSql.getTable().getColumns().length) ;
		ps.addBatch();
	}
	/**
	 * Returns success bio objects
	 * @return
	 */
	public ArrayList<BioObject> getSuccess() {
		return success;
	}
	/**
	 * Returns failed bio objects or return 0 from sql
	 * @return
	 */
	public ArrayList<BioObject> getFailed() {
		return failed;
	}
	/**
	 * Returns failed bio objects due to exception
	 * @return
	 */
	public ArrayList<BioObject> getError() {
		return error;
	}
	/**
	 * Clears all result lists such as success, failed, error
	 */
	public void clear() {
		success.clear(); 
		failed.clear(); 
		error.clear();
	}

	@Override
	public void close() {
		if (bioSql.getConnection() != null) {
			try {
				bioSql.getConnection().setAutoCommit(true);
			} catch (SQLException e) {
				
			}
		}
	}
}
