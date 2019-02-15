/**
 * @author rajab
 *
 */
module com.linkedlogics.bio.sql {
	exports com.linkedlogics.bio.sql;
	exports com.linkedlogics.bio.sql.annotation;
	
	requires transitive com.linkedlogics.bio ;
	requires transitive java.sql;
}