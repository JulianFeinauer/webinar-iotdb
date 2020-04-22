# Example Commands in the CLI

## Start Server

* Download the binaries from `http://iotdb.apache.org/#/Download`
* Extract
* Execute
    * `sbin/start-server.sh` on Mac / Linux
    * `sbin/start-server.bat` on Windows
* Alternative use Docker Containers `jfeinauer/iotdb-server:0.9.1`  
    * Important: `-p 6667:6667`

## Start Client

* Open another Terminal
* Execute
    * `sbin/start-client.sh` on Mac / Linux
    * `sbin/start-client.bat` on Windows
* Inspect Storage Groups / Databases
    * `SHOW STORAGE GROUP`
    * `SHOW TIMESERIES`
    
## Learning the Syntax

* We create a Storage Group now
    * `SET STORAGE GROUP TO root.fabrik1`
* Check
    * `SHOW STORAGE GROUP`
    * `SHOW TIMESERIES`

## Your first timeseries

* Create a timeseries
```
CREATE TIMESERIES root.fabrik1.linie01.steuerung01.status WITH DATATYPE=BOOLEAN,ENCODING=PLAIN
```
* Check
    * `SHOW TIMESERIES`
    * `SHOW DEVICES`

## Inserting Records

* Insert a record
    * `insert into root.fabrik1.linie01.steuerung01(timestamp,status) values(NOW(), true)`
* Read the Record
    * `SELECT status FROM root.fabrik1.linie01.steuerung01`

## Now, Java

* Complex Queries
    * Count
    * Group By
* Setup
```
<dependency>
    <groupId>org.apache.iotdb</groupId>
    <artifactId>iotdb-jdbc</artifactId>
    <version>0.9.1</version>
</dependency>
```