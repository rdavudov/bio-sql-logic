package com.linkedlogics.bio.sql.exception;

public class SqlException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public SqlException(String message) {
		super(message) ; 
	}
	
	public SqlException(Throwable exception) {
		super(exception) ; 
	}
	
	public SqlException(String message, Throwable exception) {
		super(message, exception) ; 
	}
}