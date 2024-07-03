package com.sparklistener.dev;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlUtils {
    
    public static void createOrReplaceTempViews(SparkSession spark) {
    
        // spark.sql("select 1 id, 'A' value union all select  2, 'B' union all select 3, 'C' ").createOrReplaceTempView("tableA");
        // spark.sql("select 1 id, 'Desc1' description  union all select   2, 'Desc2' union all select  3, 'Desc3' ").createOrReplaceTempView("tableB");
        // spark.sql("select 1 id, 'Detail1' detail union all select  2, 'Detail2' union all select  3, 'Detail3'").createOrReplaceTempView("tableC");


        Dataset<Row> tableA = spark.createDataFrame(
                java.util.Arrays.asList(
                        new TableA(1, "A"),
                        new TableA(2, "B"),
                        new TableA(3, "C")
                ),
                TableA.class
        );
        tableA.createOrReplaceTempView("tableA");

        Dataset<Row> tableB = spark.createDataFrame(
                java.util.Arrays.asList(
                        new TableB(1, "Desc1"),
                        new TableB(2, "Desc2"),
                        new TableB(3, "Desc3")
                ),
                TableB.class
        );
        tableB.createOrReplaceTempView("tableB");

        Dataset<Row> tableC = spark.createDataFrame(
                java.util.Arrays.asList(
                        new TableC(1, "Detail1"),
                        new TableC(2, "Detail2"),
                        new TableC(3, "Detail3")
                ),
                TableC.class
        );
        tableC.createOrReplaceTempView("tablec");
    }
}


