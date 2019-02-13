package com.linkedlogics.bio.sql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.linkedlogics.bio.BioDictionary;
import com.linkedlogics.bio.dictionary.builder.DictionaryReader;
import com.linkedlogics.bio.exception.DictionaryException;
import com.linkedlogics.bio.sql.dictionary.builder.AnnotationReader;
import com.linkedlogics.bio.sql.dictionary.builder.XmlReader;

public class BioDictionaryBuilder extends com.linkedlogics.bio.BioDictionaryBuilder {
	protected List<DictionaryReader> readers = new ArrayList<DictionaryReader>();
	
	
	public com.linkedlogics.bio.BioDictionaryBuilder addPackage(String packageName) {
		readers.add(new AnnotationReader()) ;
		return super.addPackage(packageName);
	}
	
	/**
	 * Adding xml file path for gathering bio obj info from xml file
	 * @param xml
	 * @return
	 */
	public BioDictionaryBuilder addTableFile(String xmlFile) {
		try {
			readers.add(new XmlReader(new FileInputStream(xmlFile))) ;
			return this ;
		} catch (FileNotFoundException e) {
			throw new DictionaryException(e) ;
		}
	}
	/**
	 * Adding xml resource name for gathering bio obj info from xml
	 * @param resource
	 * @return
	 */
	public BioDictionaryBuilder addTableResource(String resource) {
		readers.add(new XmlReader(this.getClass().getClassLoader().getResourceAsStream(resource))) ;
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
		
		BioSqlDictionary.getDictionaryMap().entrySet().stream().forEach(e -> {
			e.getValue().getCodeMap().entrySet().stream().forEach(t -> {
				t.getValue().generate(); 
			});
		});
	}
}
