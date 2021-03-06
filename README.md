# Bio SQL
Bio Sql is an SQL wrapper for Bio Objects where basic CRUD operations are supported. You only need to add ```@BioSql``` and ```@BioSqlTag``` annotations to your objects and tags. 

## Features
- Simple way of CRUD operations w/o writing queries
- Version control for objects (older version does not overwrite newer ones)
- Ability to store primitive arrays in CSV format in single column (Denormalization of arrays)
- Ability to store Bio Objects in JSON/XML format in single column (Denormalization of composed objects)
- Definition of relations of One-to-One and One-To-Many
- Enums can be stored as numbers or strings

Bio Sql requires to add two more annotations and that's all. 
- ```@BioSql``` indicates that Bio Object can be persisted to database similar to ```@Entity``` annotation in JPA
- ```@BioSqlTag``` indicates that this field must be persisted to database similar to ```@Column``` annotation in JPA

Here is an example:

```java
@BioObj
@BioSql
public class Vehicle extends BioObject {
  @BioTag(type="String")
  @BioSqlTag(isKey=true)
  public static final String VIN = "vin" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String YEAR_OF_PRODUCTION = "year_of_production" ;
  @BioTag(type="String")
  @BioSqlTag
  public static final String PRODUCER = "producer" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String ENGINE = "engine" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String CYLINDERS = "cylinders" ;
  @BioTag(type="Double")
  public static final String FUEL_EFFICIENCY = "fuel_efficiency" ;
}
```

**Note that** ```@BioSql``` must be used together with ```@BioObj``` and ```@BioSqlTag``` must be used together with ```@BioTag```.

Now we need to create Sql Dictionary in order to prepare all related SQL queries and populate relations.
```java
 new BioDictionaryBuilder().build(); 
```
 also you can specify package root for Bio Objects and Bio Sql traversal.
```java
 new BioDictionaryBuilder().addPackage("com.linkedlogics.bio.test").build();
```
Here we need to use ```com.linkedlogics.bio.sql.BioDictionaryBuilder``` instead of ```com.linkedlogics.bio.BioDictionaryBuilder```. Don't worry Bio Objects will still be processed and Bio Dictionary will be created because sql variant also extends Bio Dictionary from Bio Objects.
 
By default snake case of class and fields will be used as table name and columns. But it can be modified by properties of annotation as following:
```java
@BioObj
@BioSql(schema="test", table="vehicles")
public class Vehicle extends BioObject {
  @BioTag(type="String")
  @BioSqlTag(isKey=true, column="vin_id")
  public static final String VIN = "vin" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String YEAR_OF_PRODUCTION = "year_of_production" ;
  @BioTag(type="String")
  @BioSqlTag
  public static final String PRODUCER = "producer" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String ENGINE = "engine" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String CYLINDERS = "cylinders" ;
  @BioTag(type="Double")
  public static final String FUEL_EFFICIENCY = "fuel_efficiency" ;
}
```
There are a bunch of other properties inside ```@BioSqlTag```
- ```isKey```               indicates PK
- ```isVersion```           indicates Version tag and it will be considered during updates/merges
- ```isBlob```          indicates data should be stored as BLOB
- ```isClob```          indicates data should be stored as CLOB
- ```isJson```          indicates data should be stored as JSON
- ```isXml```               indicates data should be stored as XML
- ```isHex```               indicates data should be serialized and stored as Hex String
- ```isCompressed```        indicates data should be compressed after serialization
- ```isEncrypted```         indicates data should be encrypted after serialization
- ```isEnumAsString```  indicates Enum will be stored as String representation

## Initialization 
We need to create an instance of BioSql class as following:
```java
BioSql<Vehicle> sql = new BioSql<Vehicle>(Vehicle.class) ;
```
and also we need to set JDBC connection as following:
```java
sql.setConnection(connection) ;
```
Bio Sql doesn't provide any database connection pooling. So you have to provide sql connection from data source or pure JDBC connection.
 
## Inserting Bio Objects
Once we have BioSql instance we can create Bio Object as following:
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.VIN, "hs2123122h212") ;
v.set(Vehicle.PRODUCER, "Ford") ;
v.set(Vehicle.YEAR_OF_PRODUCTION, 2019) ;
v.set(Vehicle.FUEL_EFFICIENCY, 17.8) ;
v.set("undefined tag", "Hello world") ;

int result = sql.insert(v) ;
```
if ```result > 0``` it means that object is inserted. It will throw SQLException if something bad happens.
Only values of tags having ```@BioSqlTag``` annotation will be inserted rest will be ignored.

## Selecting Bio Objects
There are two ways of selecting. One is with single primary key, and another one with multiple keys or any condition.
```java
Vehicle selected = sql.select("hs2123122h212") ;
```
or 
```java
List<Vehicle> list = sql.select(new Vehicle() {{
    set(Vehicle.VIN, "hs2123122h212") ;
    set(Vehicle.KEY2, "12345678") ;
}}) ;
```
In order to select based on condition you have to use ```Where``` class as following:
```java
List<Vehicle> list = sql.select(null, new Where("year_of_production > ?") {{
    setInt(1, 2015) ;
}});
```

## Updating Bio Objects
You have to provide Bio Object with PK value inside as following:
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.VIN, "hs2123122h212") ;
v.set(Vehicle.FUEL_EFFICIENCY, 19.2) ;

int result = sql.update(v) ;
```
if ```result > 0``` it means that object is updated. It will throw SQLException if something bad happens.
**Note that** ```update()``` will set to NULL if any tag is missing inside Bio Object. If you want to update only some of the fields then you have to use ```merge()```

In order to update based on condition you have to use ```Where``` class as following:
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.FUEL_EFFICIENCY, 19.2) ;

List<Vehicle> list = sql.update(v, new Where("year_of_production > ?") {{
    setInt(1, 2015) ;
}});
```

## Merging Bio Objects
Merging only updates existing values inside Bio Object, remaning columns will be untouched.
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.VIN, "hs2123122h212") ;
v.set(Vehicle.FUEL_EFFICIENCY, 19.2) ;

int result = sql.merge(v) ;
```
if ```result > 0``` it means that object is updated. It will throw SQLException if something bad happens.

In order to merge based on condition you have to use ```Where``` class as following:
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.FUEL_EFFICIENCY, 19.2) ;

List<Vehicle> list = sql.merge(v, new Where("year_of_production > ?") {{
    setInt(1, 2015) ;
}});
```

## Deleting Bio Objects
You have to provide Bio Object only with PK value inside as following:
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.VIN, "hs2123122h212") ;

int result = sql.delete(v) ;
```
if ```result > 0``` it means that object is updated. It will throw SQLException if something bad happens.

In order to delete based on condition you have to use ```Where``` class as following:
```java
List<Vehicle> list = sql.delete(null, new Where("year_of_production > ?") {{
    setInt(1, 2015) ;
}});
```

## Bio SQL Versioned Update and Merge
If you are using ```isVersion``` property then it means that any update/merge will be checked for version first. Here it is how it works.
First we need to specify which tag/column is holding object's version.
```java
@BioObj
@BioSql(schema="test", table="vehicles")
public class Vehicle extends BioObject {
  @BioTag(type="String")
  @BioSqlTag(isKey=true, column="vin_id")
  public static final String VIN = "vin" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String YEAR_OF_PRODUCTION = "year_of_production" ;
  @BioTag(type="String")
  @BioSqlTag
  public static final String PRODUCER = "producer" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String ENGINE = "engine" ;
  @BioTag(type="Integer")
  @BioSqlTag
  public static final String CYLINDERS = "cylinders" ;
  @BioTag(type="Double")
  public static final String FUEL_EFFICIENCY = "fuel_efficiency" ;
  
  @BioTag(type="Integer")
  @BioSqlTag(isVersion=true)
  public static final String VERSION = "version" ;
}
```
Here we have added new tag ```VERSION``` you can also name it with different name. And we also add ```@BioSqlTag``` with ```isVersion=true```. Now BioSql knows that it should check version before update and merge.

So during update if ```version < :version``` condition is satisfied update/merge is performed. For example:
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.VIN, "hs2123122h212") ;
v.set(Vehicle.FUEL_EFFICIENCY, 19.2) ;
v.set(Vehicle.VERSION, 1) ;

int result = sql.update(v) ;
int result2 = sql.update(v) ;

v.set(Vehicle.VERSION, v.getInt(Vehicle.VERSION) + 1) ;
int result3 = sql.update(v) ;
```
Here ```result``` will be 1 and ```result2``` will be 0 because same version has already been updated. And ```result3``` will be 1 again because before update we have increased version. 

### It is very useful feature when you are developing distributed and fault tolerant systems and want to keep your system data consistent.

**Note that** there can only be one version tag for a Bio Object.

Also if you want to bypass versioning and update object anyway but don't know version. You can set version as -1 and system will ignore version checking for this particular update. For Example:
```java
Vehicle v = new Vehicle() ;
v.set(Vehicle.VIN, "hs2123122h212") ;
v.set(Vehicle.FUEL_EFFICIENCY, 19.2) ;
v.set(Vehicle.VERSION, -1) ;

int result = sql.update(v) ;
int result2 = sql.update(v) ;

```
Here both ```result``` and ```result2``` returns 1 because we have skipped version control. Version remains same but data is being updated or merged.
