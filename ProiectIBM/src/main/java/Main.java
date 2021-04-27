
import org.apache.calcite.util.Util;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.Properties;

import static org.apache.spark.sql.functions.*;


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
        String url="jdbc:mysql://127.0.0.1:3306/GeorgeDB?";
        Properties prop= new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password", "123456");


      data.write().mode(SaveMode.Overwrite)
               .jdbc(url,"tabel2",prop);

            data.show();
       Dataset<Row> results=data.groupBy(col("School unit name"),col("Gender")).agg(sum("Elementary school cases")
       ,sum("Middle school cases"),sum("High school cases"));
       Dataset<Row> elementarySc=data.groupBy(col("School unit name"),col("Gender"))
               .agg(sum("Elementary school cases"));
        Dataset<Row> middleSc=data.groupBy(col("School unit name"),col("Gender"))
                .agg(sum("Middle school cases"));
        Dataset<Row> highSc=data.groupBy(col("School unit name"),col("Gender"))
                .agg(sum("High school cases"));
        results.filter(col("School unit name").equalTo(lit("Scoala"))).show();
//        elementarySc.join(middleSc,elementarySc.col("School unit name").equalTo(middleSc.col("School unit name"))
//                .and(elementarySc.col("Gender")).equalTo(middleSc.col("Gender"))).show();
            elementarySc.join(middleSc,"Gender").show(1000);

        spark.stop();


}

}