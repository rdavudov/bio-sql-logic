/**
 * @author rajab
 *
 */
module bio.sql {
	exports com.linkedlogics.bio.sql;
	exports com.linkedlogics.bio.sql.annotation;
	
	requires transitive bio.object ;
	requires transitive java.sql;
}