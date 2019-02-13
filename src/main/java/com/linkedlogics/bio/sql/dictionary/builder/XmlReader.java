package com.linkedlogics.bio.sql.dictionary.builder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.linkedlogics.bio.BioDictionary;
import com.linkedlogics.bio.BioDictionaryBuilder;
import com.linkedlogics.bio.BioEnum;
import com.linkedlogics.bio.BioFunction;
import com.linkedlogics.bio.dictionary.BioEnumObj;
import com.linkedlogics.bio.dictionary.BioFunc;
import com.linkedlogics.bio.dictionary.BioObj;
import com.linkedlogics.bio.dictionary.BioTag;
import com.linkedlogics.bio.dictionary.BioType;
import com.linkedlogics.bio.dictionary.builder.DictionaryReader;
import com.linkedlogics.bio.exception.DictionaryException;
import com.linkedlogics.bio.exception.ParserException;

public class XmlReader implements DictionaryReader {
	private InputStream in ;
	
	public XmlReader(InputStream in) {
		this.in = in ;
	}
	
	@Override
	public void read(BioDictionaryBuilder builder) {
		// TODO Auto-generated method stub
	}

	/**
	 * Parses bio from xml string
	 * @param xml
	 * @return
	 */
    public BioDictionary parse(String xml) {
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
    public BioDictionary parse(InputStream in) {
    	try (in) {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(in);
			doc.getDocumentElement().normalize();
//			return parseDictionary(doc.getFirstChild()) ;
			return null ;
		} catch (Throwable e) {
			throw new ParserException(e) ;
		}
    }

}
