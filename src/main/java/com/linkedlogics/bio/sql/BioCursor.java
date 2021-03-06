package com.linkedlogics.bio.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import com.linkedlogics.bio.BioObject;
import com.linkedlogics.bio.sql.exception.SqlException;
import com.linkedlogics.bio.sql.utility.SqlUtility;

/**
 * Iterator for iterating large sets of bio object result
 * @author rdavudov
 *
 * @param <T>
 */
public class BioCursor<T extends BioObject> implements Iterator<T> {
	private BioSql<T> sql ;
	private ResultSet rs ;
	private PreparedStatement ps ;
	
	public BioCursor(BioSql<T> sql, ResultSet rs, PreparedStatement ps) {
		this.sql = sql ;
		this.rs = rs ;
		this.ps = ps ;
	}
	
	/**
	 * Returns true if the iteration has more elements.
	 */
	public boolean hasNext() {
		if (rs != null) {
			try {
				if (rs.next()) {
					return true ;
				} else {
					rs.close(); 
					ps.close(); 
				}
			} catch (SQLException e) {
				throw new SqlException(e) ;
			}
		}
		return false;
	}

	/**
	 * Returns the next bio object in the iteration.
	 */
	public T next() {
		try {
			T newObject = (T) sql.create() ;
			int index = 0 ;
			for (int i = 0; i < sql.getTable().getColumns().length; i++) {
				index = index + 1 ;
				Object value = SqlUtility.getParameter(rs, index, sql.getTable().getColumns()[i], sql.getBinaryParser(), sql.getXmlParser()) ;
				if (value != null) {
					newObject.put(sql.getTable().getColumns()[i].getTagName(), value) ;
				}
			}
			
			if (!sql.isLazy()) {
				sql.selectRelations(newObject);
			}
			
			return newObject ;
		} catch (Throwable e) {
			throw new SqlException(e) ;
		}
	}
}
