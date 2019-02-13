package com.linkedlogics.bio.sql.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Repeatable(value=BioRemoteSqlTags.class)
public @interface BioRemoteSqlTag {
	String obj() ;
	String column() default "" ;
	boolean isKey() default false ;
	boolean isVersion() default false ; // is used in updating objects where we want version control
	boolean isBlob() default false;									// so that older versions can't overwrite newer ones but vice versa is possible
	boolean isClob() default false;
	boolean isJson() default false;		
	boolean isXml() default false;
	boolean isHex() default false;
	boolean isAuto() default false;
	boolean isRelation() default false ;
	boolean isCompressed() default false ;
	boolean isEncrypted() default false ;
	boolean isEnumAsString() default false ;
}
