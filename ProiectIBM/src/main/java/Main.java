
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.Properties;

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
        String url="jdbc:mysql://127.0.0.1:3306/GeorgeDB?user=root;password=123456";
        Properties prop= new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password", "123456");


       data.write().mode("append")
               .jdbc(url,"tabel2",prop);

    System.out.println("Merge branch1");

       data.show(50);
        spark.stop();

}

}