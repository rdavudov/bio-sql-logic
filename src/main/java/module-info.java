/**
 * 
 */
/**
 * @author rajab
 *
 */
module com.linkedlogics.bio.sql {
	exports com.linkedlogics.bio.sql;
	exports com.linkedlogics.bio.sql.annotation;

	requires com.linkedlogics.bio;
	requires io.github.classgraph;
	requires java.sql;
	requires java.xml;
	requires org.json;
}