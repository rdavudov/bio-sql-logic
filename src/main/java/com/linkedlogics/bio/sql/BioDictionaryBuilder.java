package com.linkedlogics.bio.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.linkedlogics.bio.dictionary.builder.DictionaryReader;
import com.linkedlogics.bio.exception.DictionaryException;
import com.linkedlogics.bio.sql.dictionary.builder.AnnotationReader;
import com.linkedlogics.bio.sql.dictionary.builder.XmlReader;
import com.linkedlogics.bio.sql.object.BioTable;

/**
 * Dictionary builder for sql tables
 * @author rajab
 *
 */
public class BioDictionaryBuilder extends com.linkedlogics.bio.BioDictionaryBuilder {
	protected List<DictionaryReader> readers = new ArrayList<DictionaryReader>();
	
	@Override
	public BioDictionaryBuilder addPackage(String packageName) {
		readers.add(new AnnotationReader(packageName)) ;
		super.addPackage(packageName);
		return this ;
	}
	
	/**
	 * Adding xml file path for gathering bio obj info from xml file
	 * @param xml
	 * @return
	 */
	public BioDictionaryBuilder addTableFile(String xmlFile) {
		File f = new File(xmlFile) ;
		if (f.exists()) {
			try {
				readers.add(new XmlReader(new FileInputStream(xmlFile))) ;
			} catch (FileNotFoundException e) { }
		} else {
			readers.add(new XmlReader(this.getClass().getClassLoader().getResourceAsStream(xmlFile))) ;
		}
		
		return this ;
	}
	/**
	 * Adding url resource name for gathering bio obj info from xml
	 * @param url
	 * @return
	 */
	public BioDictionaryBuilder addTableUrl(String url) {
		try {
			readers.add(new XmlReader(new URL(url).openStream())) ;
		} catch (Throwable e) {
			throw new DictionaryException(e) ;
		}
		return this ;
	}
	
	/**
	 * Must be called at first because it constructs all dictionary can be found in class path, URL path etc.
	 */
	@Override
	public void build() {
		super.build(); 
		
		if (readers.size() == 0) {
			readers.add(new AnnotationReader()) ;
		}
		
		for (DictionaryReader reader : readers) {
			reader.read(this); 
		}
		
		
		for (Entry<Integer, BioSqlDictionary> e : BioSqlDictionary.getDictionaryMap().entrySet()) {
			for (Entry<Integer, BioTable> t : e.getValue().getCodeMap().entrySet()) {
				t.getValue().generate(); 
			}
		}
		
		for (Entry<Integer, BioSqlDictionary> e : BioSqlDictionary.getDictionaryMap().entrySet()) {
			for (Entry<Integer, BioTable> t : e.getValue().getCodeMap().entrySet()) {
				t.getValue().generateRelations(); 
			}
		}
	}
}
