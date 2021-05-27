package app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import java.util.Properties;

public class ToolDB {
    String url="jdbc:mysql://127.0.0.1:3306/GeorgeDB?";
    Properties prop= new Properties();
    {prop.setProperty("user","root");
        prop.setProperty("password", "123456");
        prop.setProperty("driver","com.mysql.cj.jdbc.Driver");
    }
    public  void write(Dataset<Row> d)
    {


        d.write().mode(SaveMode.Overwrite)
                .jdbc(url,"Covid191",prop);
    }
    public Dataset<Row> read(SparkSession spark)
    {

        try {
            return spark.read().jdbc(url, "Covid191", prop);
        } catch (Exception e) {
            return spark.emptyDataFrame();
        }


    }

}
