package com.linkedlogics.bio.sql.object;

import com.linkedlogics.bio.dictionary.BioTag;
import com.linkedlogics.bio.sql.Where;
/**
 * Defines a relation between two tables through keys
 * @author rajab
 *
 */
public class BioRelation {
	// Tag for related bio object
	private BioTag tag ;
	// local keys as FKs
	private String[] relateKeys ;
	// related keys as PKs of that table
	private String[] toKeys ;
	// indicates whether relation is 1-1 or 1-*
	private boolean isMany ;
	// keeps where clause for relation objects
	private Where where ;
	
	public BioRelation(BioTag tag) {
		this.tag = tag ;
		this.isMany = tag.isArray() || tag.isList() ;
	}
	
	
	public boolean isMany() {
		return isMany;
	}

	public void setMany(boolean isMany) {
		this.isMany = isMany;
	}

	public BioTag getTag() {
		return tag;
	}
	
	public void setTag(BioTag tag) {
		this.tag = tag;
	}
	
	public String[] getRelateKeys() {
		return relateKeys;
	}
	
	public void setRelateKeys(String[] relateKeys) {
		this.relateKeys = relateKeys;
	}
	
	public String[] getToKeys() {
		return toKeys;
	}
	
	public void setToKeys(String[] toKeys) {
		this.toKeys = toKeys;
	}

	public Where getWhere() {
		return where;
	}

	public void setWhere(Where where) {
		this.where = where;
	}
}
