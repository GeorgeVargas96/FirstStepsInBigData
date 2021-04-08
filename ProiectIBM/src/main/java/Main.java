
import org.apache.spark.sql.SparkSession;
import org.apache.spark.*;
public class Main {
    public static void main(String []args)
    {
        SparkSession app= SparkSession.builder()
                                        .appName("BigData")
                                            .config("spark.master","local").getOrCreate();

        System.out.println("Hello");
        app.stop();

}

}