# HiveUDTF
This Hive UDTF will duplicate the first input column

## a. How to build the jar

```shell
mvn package
```

##b. Prepare a Hive table with sample data

In Hive CLI, create a test table:

```sql
create table testudtf(a string, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";
```

Put below data for above Hive table:

```shell
echo "abc,xyz" > test.csv
```

##c. Test UDTF

```sql
ADD JAR ~/target/DoubleColumn-1.0.0.jar;
CREATE TEMPORARY FUNCTION double_column AS 'openkb.hive.udtf.DoubleColumn'; 
SELECT double_column(a,b) as (a1,a2,b) FROM testudtf;

Result:
abc	abc	xyz
```
