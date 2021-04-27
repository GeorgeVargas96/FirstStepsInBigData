import org.apache.spark.sql.SparkSession;

public class CreareSs {

    public static SparkSession start()
    {
        System.setProperty("hadoop.home.dir", System.getProperty("user.home"));
        return SparkSession.builder()
                .appName("BigData")
                .config("spark.master","local[*]").getOrCreate();

    }
}
