package blackfriday;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;


public class BlackFridayJavaProgramming {
    public static void main(String[] args) {
        // Accessing Spark Cluster
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sankir").getOrCreate();
        // reading Input file part-1
        Dataset<Row> inp = sparkSession.read().format("csv").option("header", "true").load("C:\\Users\\Merajuddin Sheikh\\Downloads\\BlackFriday.csv");


        SQLContext sqlContext = new SQLContext(sparkSession);

        inp.createOrReplaceTempView("BlackFriday");

        // Dataset<Row> out = sparkSession.sql("select User_ID, sum(Purchase) from BlackFriday group by User_ID having User_ID=1000001");

        JavaRDD<Row> rdd1 = inp.toJavaRDD();
          // Removing bad records
        JavaRDD<Row> rdd2 = rdd1.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                if (row.anyNull())
                    return false;
                else
                    return true;
            }
        });
        // Action applied
        System.out.println(rdd2.count());

        StructType schema = rdd2.first().schema();

        System.out.println(schema);

        //Creating dataframe

        Dataset<Row> df1 = sqlContext.createDataFrame(rdd2, schema);

        df1.createOrReplaceTempView("CSV_Table");

        Dataset<Row> ds1 = sparkSession.sql("select User_ID,sum(Purchase) from CSV_Table group by User_ID having User_ID=1000001");

        ds1.show();

        // Reading Second part of a file.
        Dataset<Row> jsonData1 = sparkSession.read().schema(schema).json("C:\\Users\\Merajuddin Sheikh\\Downloads\\BlackFriday1.json");

        jsonData1.show();

        JavaRDD<Row> jsonRDD1 = jsonData1.toJavaRDD();

        System.out.println(jsonRDD1.count());

        // Removing bad records from file2.
        JavaRDD<Row> jsonRDD2 = jsonRDD1.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                if (row.anyNull())
                    return false;
                else
                    return true;
            }
        });

        System.out.println(jsonRDD2.count());

        StructType schema2 = jsonRDD2.first().schema();

        //Creating dataframe

        Dataset<Row> df2 = sqlContext.createDataFrame(jsonRDD2, schema2);

        df2.createOrReplaceTempView("JSON_Table1");

        ds1 = sparkSession.sql("select User_ID,sum(Purchase) from JSON_Table1 group by User_ID having User_ID=1000001");

        ds1.show();

        Dataset<Row> jsonData2 = sparkSession.read().schema(schema).json("C:\\Users\\Merajuddin Sheikh\\Downloads\\BlackFriday2.json");

        jsonData2.show();

        JavaRDD<Row> jsonRDD3 = jsonData2.toJavaRDD();

        System.out.println(jsonRDD3.count());

        JavaRDD<Row> jsonRDD4 = jsonRDD3.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                if (row.anyNull())
                    return false;
                else
                    return true;
            }
        });

        System.out.println(jsonRDD4.count());

        StructType schema3 = jsonRDD4.first().schema();
        // Creating dataframe
        Dataset<Row> df3 = sqlContext.createDataFrame(jsonRDD4, schema3);

        df3.createOrReplaceTempView("JSON_Table2");

        ds1 = sparkSession.sql("select User_ID,sum(Purchase) from JSON_Table2 group by User_ID having User_ID=1000001");
        // Action applied
        ds1.show();

        Dataset<Row> ds6 = sparkSession.sql("select * from CSV_Table union select * from JSON_Table1 union select * from JSON_Table2");
        // Action applied
        System.out.println(ds6.count());

        ds1.createOrReplaceTempView("Output1");

        Dataset<Row> ds2 = sparkSession.sql("select User_ID, sum(Purchase) from Output1 group by User_ID having USER_ID=1000001");
        // Action applied
        ds2.show();

        Dataset<Row> ds3 = sparkSession.sql("select distinct User_ID from Output1 order by User_ID");

        // Action applied  and WritingOutput to a file

        ds2.coalesce(1).write().csv("C:\\Black");

        ds3.coalesce(1).write().csv("C:\\BLACFRIDAY_OUT");


    }
}
