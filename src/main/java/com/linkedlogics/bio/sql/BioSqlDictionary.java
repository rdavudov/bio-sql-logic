package com.linkedlogics.bio.sql;

import java.util.HashMap;

import com.linkedlogics.bio.exception.DictionaryException;

public class BioSqlDictionary {
	private int code ;
	
	 /**
     * Map for retrieving BioTable based on object code
     */
    private HashMap<Integer, BioTable> codeMap = new HashMap<Integer, BioTable>();
    /**
     * Map for retrieving BioTable based on object type
     */
    private HashMap<String, BioTable> typeMap = new HashMap<String, BioTable>();
    /**
     * Map for retrieving BioTable based on object name
     */
    private HashMap<String, BioTable> nameMap = new HashMap<String, BioTable>();
    
    BioSqlDictionary() {

    }
    
    BioSqlDictionary(int code) {
    	this.code = code ;
    }
    
    public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}
	
    HashMap<Integer, BioTable> getCodeMap() {
		return codeMap;
	}

	HashMap<String, BioTable> getTypeMap() {
		return typeMap;
	}

	HashMap<String, BioTable> getNameMap() {
		return nameMap;
	}

	public void addTable(BioTable table) {
    	BioTable tableByCode = codeMap.get(table.getObj().getCode());
    	BioTable tableByName = typeMap.get(table.getObj().getType());

        if (tableByCode == null && tableByName == null) {
            codeMap.put(table.getObj().getCode(), table);
            typeMap.put(table.getObj().getType(), table);
            nameMap.put(table.getObj().getName(), table) ;
        } else if (tableByCode != tableByName) {
//            if (objByCode == null) {
//                throw new DictionaryException("already existing name " + obj.getType() + " in dictionary with different code " + objByName.getCode());
//            } else if (objByName == null) {
//                throw new DictionaryException("already existing code " + obj.getCode() + " in dictionary with different name " + objByCode.getType());
//            } else if (objByCode.getCode() != obj.getCode()) {
//                throw new DictionaryException("already existing name " + obj.getType() + " in dictionary with different code " + objByCode.getCode());
//            } else if (!objByName.getType().equals(obj.getType())) {
//                throw new DictionaryException("already existing code " + obj.getCode() + " in dictionary with different name " + objByName.getCode());
//            }
        } else if (tableByCode.getObj().getBioClass().isAssignableFrom(table.getObj().getBioClass()) || tableByCode.getObj().getBioClass() == table.getObj().getBioClass()) {
        	 codeMap.put(table.getObj().getCode(), table);
             typeMap.put(table.getObj().getType(), table);
             nameMap.put(table.getObj().getName(), table) ;
        }
    }
    
    public BioTable getTableByType(String type) {
        return typeMap.get(type);
    }
    
    public BioTable getTableByName(String name) {
        return nameMap.get(name);
    }

    public BioTable getTableByCode(int code) {
        return codeMap.get(code);
    }
    
	private static HashMap<Integer, BioSqlDictionary> dictionaryMap = new HashMap<Integer, BioSqlDictionary>() ;
	
    private static BioSqlDictionary dictionary ;
 
    /**
     * Returns all dictionaries as map
     * @return
     */
    static HashMap<Integer, BioSqlDictionary> getDictionaryMap() {
		return dictionaryMap;
	}

    /**
     * Returns default dictionary
     * @return
     */
	public static BioSqlDictionary getDictionary() {
        return dictionary ;
    }
    
	/**
	 * Returns dictionary and creates an empty one if it is not found
	 * @param dictionary
	 * @return
	 */
    public static BioSqlDictionary getOrCreateDictionary(int dictionary) {
    	BioSqlDictionary d = dictionaryMap.get(dictionary) ;
    	if (d == null) {
    		synchronized(dictionaryMap) {
    			if (!dictionaryMap.containsKey(dictionary)) {
	    			d = new BioSqlDictionary() ;
	    			d.setCode(dictionary);
	    			dictionaryMap.put(dictionary, d) ;
	    			if (dictionary == 0) {
	    				BioSqlDictionary.dictionary = d ;
	    			}
	    			return d ;
    			} else {
    				return dictionaryMap.get(dictionary) ;
    			}
    		}
    	}
    	return d ;
    }
    
    /**
     * Returns dictionary by id
     * @param dictionary
     * @return
     */
    public static BioSqlDictionary getDictionary(int dictionary) {
    	BioSqlDictionary dict = dictionaryMap.get(dictionary) ;
    	if (dict == null) {
    		throw new DictionaryException(dictionary + " sql dictionary is not found") ;
    	}
    	return dict ;
    }
   
}
