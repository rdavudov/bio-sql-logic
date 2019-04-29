package com.linkedlogics.bio.sql.dictionary.builder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Types;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.linkedlogics.bio.BioDictionaryBuilder;
import com.linkedlogics.bio.dictionary.builder.DictionaryReader;
import com.linkedlogics.bio.exception.ParserException;
import com.linkedlogics.bio.sql.BioSqlDictionary;
import com.linkedlogics.bio.sql.object.BioColumn;
import com.linkedlogics.bio.sql.object.BioRelation;
import com.linkedlogics.bio.sql.object.BioTable;
import com.linkedlogics.bio.sql.utility.DictionaryUtility;
import com.linkedlogics.bio.sql.utility.SqlUtility;

public class XmlReader implements DictionaryReader {
	private InputStream in ;
	
	public XmlReader(InputStream in) {
		this.in = in ;
	}
	
	@Override
	public void read(BioDictionaryBuilder builder) {
		parse(in) ;
	}

	/**
	 * Parses bio from xml string
	 * @param xml
	 * @return
	 */
    public BioSqlDictionary parse(String xml) {
        try {
			return parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			throw new ParserException(e) ;
		}
    }

    /**
     * Parses bio from input stream
     * @param in
     * @return
     */
    public BioSqlDictionary parse(InputStream in) {
    	try (in) {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(in);
			doc.getDocumentElement().normalize();
			return parseDictionary(doc.getFirstChild()) ;
		} catch (Throwable e) {
			throw new ParserException(e) ;
		}
    }

    protected BioSqlDictionary parseDictionary(Node e) {
    	NamedNodeMap atts = e.getAttributes();

    	int code = 0 ;
    	for (int i = 0; i < atts.getLength(); i++) {
    		Node node = atts.item(i);
    		if ("code".contentEquals(node.getNodeName())) {
    			code = Integer.parseInt(node.getNodeValue()) ;
    		} 
    	}
    	BioSqlDictionary dictionary = BioSqlDictionary.getOrCreateDictionary(code) ;
    	
    	 NodeList nodes = e.getChildNodes() ;
         for (int i = 0; i < nodes.getLength(); i++) {
         	Node node = nodes.item(i);
         	if (node.getNodeType() == Node.ELEMENT_NODE) {
         		if (node.getNodeName().contentEquals("table")) {
         			BioTable table = parseTable(node, dictionary.getCode()) ;
         			if (table != null) {
         				dictionary.addTable(table) ;
         			}
         		}
         	}
         }
    	
    	return dictionary ;
    }

    protected BioTable parseTable(Node e, int dictionary) {
    	NamedNodeMap atts = e.getAttributes();
    	int code = 0 ;
    	String name = null ;
    	String type = null ;
    	String schema = null ;
  
    	for (int i = 0; i < atts.getLength(); i++) {
    		Node node = atts.item(i);
    		if ("code".contentEquals(node.getNodeName())) {
    			code = Integer.parseInt(node.getNodeValue()) ;
    		} else if ("name".contentEquals(node.getNodeName())) {
    			name = node.getNodeValue() ;
    		} else if ("type".contentEquals(node.getNodeName())) {
    			type = node.getNodeValue() ;
    		} else if ("schema".contentEquals(node.getNodeName())) {
    			schema = node.getNodeValue() ;
    		}  
    	}
    	
    	BioTable table = new BioTable(dictionary, code, name, schema) ;
    	
    	NodeList nodes = e.getChildNodes() ;
        for (int i = 0; i < nodes.getLength(); i++) {
        	Node node = nodes.item(i);
        	if (node.getNodeType() == Node.ELEMENT_NODE) {
        		if (node.getNodeName().contentEquals("column")) {
        			BioColumn column = parseColumn(node, table) ;
        			if (column != null) {
        				table.addColumn(column); 
        			}
        		} else if (node.getNodeName().contentEquals("relation")) {
        			BioRelation relation = parseRelation(node, table) ;
        			if (relation != null) {
        				table.addRelation(relation); 
        			}
        		}
        	}
        }
    	
    	return table ;
    }
    
    public BioColumn parseColumn(Node e, BioTable table) {
    	NamedNodeMap atts = e.getAttributes();
    	int tagCode = 0 ;
    	String name = null ;
    	String type = null ;
    	boolean isArray = false ;
    	boolean isList = false ;
    	boolean isMandatory = false ;
    	boolean isKey = false ;
    	boolean isVersion = false ;
    	boolean isBlob = false ;
    	boolean isClob = false ;
    	boolean isXml = false ;
    	boolean isJson = false ;
    	boolean isHex = false ;
    	boolean isCompressed = false ;
    	boolean isEncrypted = false ;
    	boolean isEnumAsString = false ;
    	
    	for (int i = 0; i < atts.getLength(); i++) {
    		Node node = atts.item(i);
    		if ("tag-code".contentEquals(node.getNodeName())) {
    			tagCode = Integer.parseInt(node.getNodeValue()) ;
    		} else if ("type".contentEquals(node.getNodeName())) {
    			type = node.getNodeValue() ;
    		} else if ("name".contentEquals(node.getNodeName())) {
    			name = node.getNodeValue() ;
    		} else if ("is-array".contentEquals(node.getNodeName())) {
    			isArray = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-list".contentEquals(node.getNodeName())) {
    			isList = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-mandatory".contentEquals(node.getNodeName())) {
    			isMandatory = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-key".contentEquals(node.getNodeName())) {
    			isKey = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-version".contentEquals(node.getNodeName())) {
    			isVersion = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-blob".contentEquals(node.getNodeName())) {
    			isBlob = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-clob".contentEquals(node.getNodeName())) {
    			isClob = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-xml".contentEquals(node.getNodeName())) {
    			isXml = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-json".contentEquals(node.getNodeName())) {
    			isJson = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-hex".contentEquals(node.getNodeName())) {
    			isHex = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-compressed".contentEquals(node.getNodeName())) {
    			isCompressed = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-encrypted".contentEquals(node.getNodeName())) {
    			isEncrypted = Boolean.parseBoolean(node.getNodeValue()) ;
    		} else if ("is-enum-as-string".contentEquals(node.getNodeName())) {
    			isEnumAsString = Boolean.parseBoolean(node.getNodeValue()) ;
    		}  
    	}

    	BioColumn column ;
    	if (tagCode > 0) {
    		column = new BioColumn(table.getObj().getTag(tagCode)) ;
    		if (name != null) {
    			column.setColumn(name);
    		} else {
    			column.setColumn(column.getTag().getName());
    		}
    		column.setSqlType(SqlUtility.getSqlType(column.getTag().getType())) ;
    	} else {
    		column = new BioColumn(name, DictionaryUtility.getSqlType(type)) ;
    		column.setArray(isArray);
    		column.setList(isList);
    		column.setMandatory(isMandatory);
    	}
    	
    	if (isBlob) 
			column.setSqlType(Types.BLOB) ;
		else if (isClob)
			column.setSqlType(Types.CLOB) ;
		else if (column.getTag() != null)
			column.setSqlType(SqlUtility.getSqlType(column.getTag().getType())) ;
		else 
			column.setSqlType(DictionaryUtility.getSqlType(type)) ;
    	
    	column.setKey(isKey);
    	column.setVersion(isVersion);
    	column.setBlob(isBlob);
    	column.setClob(isClob);
    	column.setXml(isXml);
    	column.setJson(isJson);
    	column.setHex(isHex);
    	column.setCompressed(isCompressed);
    	column.setEncrypted(isEncrypted);
    	column.setEnumAsString(isEnumAsString);
    	
    	return column ;
    }
    
    public BioRelation parseRelation(Node e, BioTable table) {
    	NamedNodeMap atts = e.getAttributes();
    	int tagCode = 0 ;
    	String relateKey = null ;
    	String toKey = null ;
    	String relateKeys = null ;
    	String toKeys = null ;
    	
    	for (int i = 0; i < atts.getLength(); i++) {
    		Node node = atts.item(i);
    		if ("tag-code".contentEquals(node.getNodeName())) {
    			tagCode = Integer.parseInt(node.getNodeValue()) ;
    		} else if ("relate-key".contentEquals(node.getNodeName())) {
    			relateKey = node.getNodeValue() ;
    		} else if ("to-key".contentEquals(node.getNodeName())) {
    			toKey = node.getNodeValue() ;
    		} else if ("relate-keys".contentEquals(node.getNodeName())) {
    			relateKeys = node.getNodeValue() ;
    		} else if ("to-keys".contentEquals(node.getNodeName())) {
    			toKeys = node.getNodeValue() ;
    		}  
    	}

    	if (tagCode > 0) {
    		BioRelation relation = new BioRelation(table.getObj().getTag(tagCode)) ;
    		if (relateKey != null && toKey != null) {
    			relation.setRelateKeys(new String[] {relateKey});
    			relation.setToKeys(new String[] {toKey});
    		} else {
    			relation.setRelateKeys(relateKeys.split(","));
    			relation.setToKeys(toKeys.split(","));
    		}
    		
    		return relation ;
    	}
    	
    	return null ;
    }
}
