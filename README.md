# uncommons-dbutils

A modernised fork of [Apache Commons DBUtils](http://commons.apache.org/proper/commons-dbutils/). The aim is to make a more flexible API and fix previous design mistakes, while keeping the original's small size and tight focus.

Full documentation is at https://mwanji.github.io/uncommons-dbutils

[![Build Status](https://img.shields.io/travis/mwanji/uncommons-dbutils.svg)](https://travis-ci.org/mwanji/uncommons-dbutils) [![Coverage Status](https://img.shields.io/coveralls/mwanji/uncommons-dbutils.svg)](https://coveralls.io/r/mwanji/uncommons-dbutils?branch=master) [![License](https://img.shields.io/badge/license-Apache%20v2.0-lightgrey.svg)](http://www.apache.org/licenses/LICENSE-2.0)

## Features

* Transaction support
* Asynchronous querying via [CompletableFuture](http://www.nurkiewicz.com/2013/05/java-8-definitive-guide-to.html)
* Lightweight: executes queries and maps `ResultSet`s to objects. That's all.
* Basic debug logging with [SLF4J](http://www.slf4j.org)
* A small number of easily-composable interfaces
* Takes advantage of Java 8 lambdas for concise code
* Tested against an in-memory database, rather than against mocks

## Installation

Requires Java 8.

For Maven, add the following dependency:

```xml
<dependency>
  <groupId>com.moandjiezana.dbutils</groupId>
  <artifactId>uncommons-dbutils</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```
See [instructions](https://mwanji.github.io/uncommons-dbutils/dependency-info.html) for other dependency management systems.

## Examples

```java
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.list;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.map;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.single;
import static com.moandjiezana.uncommons.dbutils.RowProcessor.firstColumn;
import static com.moandjiezana.uncommons.dbutils.RowProcessor.mapToFields;
import static com.moandjiezana.uncommons.dbutils.RowProcessor.mapToBean;

try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:")) {
  QueryRunner queryRunner = QueryRunner.create(connection);
  
  // Insert a record and retrieve its primary key
  Long newId = queryRunner.insert("INSERT INTO persons(name) VALUES(?)", single(firstColumn()), "John");
  
  // Update a record
  int affectedRows = queryRunner.execute("UPDATE persons SET name = ? WHERE id = ?", "Thabiso", newId);
  
  // Get a single column from a query that returns a single row
  String name = queryRunner.select("SELECT name FROM persons WHERE id = ?", single(firstColumn()), 1L);
  
  // Get a single column from a query that returns multiple rows
  List<String> names = queryRunner.select("SELECT name FROM persons", list(firstColumn()), 1L);
  
  // Create a Map containing all the single-row query's columns, i.e. { "id": 1, "name": "John" }
  Map<String, Object> person = queryRunner.select("SELECT * FROM persons WHERE id = ?", single(new MapRowProcessor(), 1L);
  
  // Create a custom Person object for each row returned by the query
  List<Person> persons = queryRunner.select("SELECT * FROM people", list(mapToFields(Person.class)));
  
  // Create a custom Address object for each row, this time using JavaBean-style access (includes support for @ConstructorProperties)
  List<Address> addresses = queryRunner.select("SELECT * FROM addresses", list(mapToBean(Address.class)));
  
  // Create a Map of primary keys to Person objects, i.e. { 1: Person(...), 2: Person(...) }
  Map<Long, Person personsMap = queryRunner.select("SELECT * FROM persons", ResultSetHandler.map("id", Long.class, personRowProcessor));
}
```

## License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Further Reading

Find out more at https://mwanji.github.io/uncommons-dbutils
