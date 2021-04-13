
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {

    public static void main(String []args)
    {
        System.setProperty("hadoop.home.dir", System.getProperty("user.home"));
        SparkSession spark=SparkSession.builder()
                                        .appName("BigData")
                                            .config("spark.master","local[*]").getOrCreate();
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("School unit name",  DataTypes.StringType, false),
                DataTypes.createStructField("Elementary school cases", DataTypes.IntegerType, true),
                DataTypes.createStructField("Middle school cases", DataTypes.IntegerType, true),
                DataTypes.createStructField("High school cases", DataTypes.IntegerType, true),
                DataTypes.createStructField("Gender", DataTypes.StringType, true),
                DataTypes.createStructField("Reporting date",DataTypes.DateType,true)
        });

       Dataset<Row> data= spark.read().format("csv")
                                    .option("header","true").schema(schema)
                                     .load("ProiectIBM/data.csv");

       data.printSchema();

       data.show(50);

        System.out.println("Hello");
        System.out.println(System.getProperty("user.home"));
        spark.stop();

}

}