
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String []args)
    {
        System.setProperty("hadoop.home.dir", System.getProperty("user.home"));
        SparkSession spark=SparkSession.builder()
                                        .appName("BigData")
                                            .config("spark.master","local[*]").getOrCreate();

       Dataset<Row> data= spark.read().format("csv")
                                    .option("header","true")
                                     .load("ProiectIBM/data.csv");
       data.printSchema();

       data.show(50);

        System.out.println("Hello");
        System.out.println(System.getProperty("user.home"));
        spark.stop();

}

}