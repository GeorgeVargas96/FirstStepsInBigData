
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String []args)
    {
        System.setProperty("hadoop.home.dir", System.getProperty("user.home"));
        SparkSession spark=SparkSession.builder()
                                        .appName("BigData")
                                            .config("spark.master","local[*]").getOrCreate();

        System.out.println("Hello");
        System.out.println(System.getProperty("user.home"));
        spark.stop();

}

}