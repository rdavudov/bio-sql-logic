package com.linkedlogics.bio.sql.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface BioRemoteSqlRelationTag {
	String obj() ;
	String relateColumn() ;
	String toColumn() ;
	String[] relateColumns() default "";
	String[] toColumns() default "";
}
