package org.pragmaticindustries.webinar.iotdb;

import org.apache.iotdb.jdbc.IoTDBSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class HelloIoTDB {

    /**
     * Before executing a SQL statement with a Statement object, you need to create a Statement object using the createStatement() method of the Connection object.
     * After creating a Statement object, you can use its execute() method to execute a SQL statement
     * Finally, remember to close the 'statement' and 'connection' objects by using their close() method
     * For statements with query results, we can use the getResultSet() method of the Statement object to get the result set.
     */
    public static void main(String[] args) throws SQLException, InterruptedException {
        Connection connection = getConnection();
        if (connection == null) {
            System.out.println("get connection defeat");
            return;
        }
        Statement statement = connection.createStatement();
        //Create storage group
        createStorageGroup(statement, "root.fabrik1");
        createStorageGroup(statement, "root.fabrik2");


        //Show storage group
        statement.execute("SHOW STORAGE GROUP");
        outputResult(statement.getResultSet());

        //Create time series
        //Different data type has different encoding methods. Here use INT32 as an example
//        create timeseries root.fabrik1.linie01.steuerung01.status with datatype=BOOLEAN,encoding=PLAIN
//        create timeseries root.fabrik1.linie02.steuerung02.hardware with datatype=TEXT,encoding=PLAIN
//        create timeseries root.fabrik1.linie01.steuerung01.temperature with datatype=FLOAT,encoding=RLE
//        create timeseries root.fabrik1.linie02.steuerung02.status with datatype=BOOLEAN,encoding=PLAIN
//        create timeseries root.fabrik2.linie03.steuerung01.status with datatype=BOOLEAN,encoding=PLAIN
//        create timeseries root.fabrik2.linie03.steuerung01.temperature with datatype=FLOAT,encoding=RLE
        createTimeseries(statement, "root.fabrik1.linie01.steuerung01.status", "BOOLEAN", "PLAIN");
        createTimeseries(statement, "root.fabrik1.linie01.steuerung01.counter", "INT32", "RLE");
        createTimeseries(statement, "root.fabrik1.linie02.steuerung02.hardware", "TEXT", "PLAIN");
        createTimeseries(statement, "root.fabrik1.linie01.steuerung01.temperature", "FLOAT", "RLE");
        createTimeseries(statement, "root.fabrik1.linie02.steuerung02.status", "BOOLEAN", "PLAIN");
        createTimeseries(statement, "root.fabrik1.linie03.steuerung01.status", "BOOLEAN", "PLAIN");
        createTimeseries(statement, "root.fabrik1.linie03.steuerung01.temperature", "FLOAT", "RLE");


        //Show time series
        statement.execute("SHOW TIMESERIES root.*");
        outputResult(statement.getResultSet());
        //Show devices
        statement.execute("SHOW DEVICES");
        outputResult(statement.getResultSet());
        //Count time series
        statement.execute("COUNT TIMESERIES root");
        outputResult(statement.getResultSet());
        //Count nodes at the given level
        statement.execute("COUNT NODES root LEVEL=3");
        outputResult(statement.getResultSet());
        //Count timeseries group by each node at the given level
        System.out.println("COUNT TIMESERIES root GROUP BY LEVEL=3");
        statement.execute("COUNT TIMESERIES root GROUP BY LEVEL=3");
        outputResult(statement.getResultSet());


        //Execute insert statements in batch

        final Instant start = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.systemDefault()).toInstant();
        final Random random = new Random();
        for (int day = 0; day <= 10; day++) {
            for (int s = 0; s <= 86400; s++) {
                final long ts = start.plus(day, ChronoUnit.DAYS).plus(s, ChronoUnit.SECONDS).toEpochMilli();
                statement.addBatch("insert into root.fabrik1.linie01.steuerung01(timestamp,counter) values(" + ts + "," + random.nextInt(100) + ");");
//                statement.addBatch("insert into root.fabrik1.linie01.steuerung01(timestamp,temperature) values(" + ts + "," + 5.0 * random.nextGaussian() + 15.0 + ");");
            }
            System.out.print(".");
            statement.executeBatch();
            statement.clearBatch();
        }

        //Full query statement
        String sql = "select * from root.fabrik1";
        ResultSet resultSet = statement.executeQuery(sql);
        System.out.println("sql: " + sql);
        outputResult(resultSet);

        //Exact query statement
        //Aggregate query
        sql = "select count(counter) from root.fabrik1.linie01.steuerung01;";
        resultSet = statement.executeQuery(sql);
        System.out.println("sql: " + sql);
        outputResult(resultSet);

        // Downsample query
//        sql = "select min_value(counter), avg(counter), max_value(counter), count(counter) from root.fabrik1.linie01.steuerung01 GROUP BY (1s, [2020-03-01T18:00:00, 2020-03-01T20:00:00]);";
        sql = "select min_value(counter), avg(counter), max_value(counter), count(counter) from root.fabrik1.linie01.steuerung01 GROUP BY (1m, [" + Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()  + ", " + Instant.now().toEpochMilli() + "]);";
        resultSet = statement.executeQuery(sql);
        System.out.println("sql: " + sql);
        outputResult(resultSet);

        //Delete time series
        // statement.execute("delete timeseries root.demo.s0");

        //close connection
        statement.close();
        connection.close();
    }

    private static void createTimeseries(Statement statement, String series, String datatype, String encoding) throws SQLException {
        try {
            statement.execute(String.format("create timeseries %s with datatype=%s,encoding=%s", series, datatype, encoding));
        } catch (IoTDBSQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createStorageGroup(Statement statement, String s) throws SQLException {
        try {
            statement.execute(String.format("set storage group to %s", s));
        } catch (IoTDBSQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static Connection getConnection() {
        // JDBC driver name and database URL
        String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
        String url = "jdbc:iotdb://127.0.0.1:6667/";

        // Database credentials
        String username = "root";
        String password = "root";

        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * This is an example of outputting the results in the ResultSet
     */
    private static void outputResult(ResultSet resultSet) throws SQLException {
        if (resultSet != null) {
            System.out.println("--------------------------");
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                System.out.print(metaData.getColumnLabel(i + 1) + " ");
            }
            System.out.println();
            while (resultSet.next()) {
                for (int i = 1; ; i++) {
                    System.out.print(resultSet.getString(i));
                    if (i < columnCount) {
                        System.out.print(", ");
                    } else {
                        System.out.println();
                        break;
                    }
                }
            }
            System.out.println("--------------------------\n");
        }
    }
}
