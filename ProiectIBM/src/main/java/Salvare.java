import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class Salvare {
    public static void salvare(Dataset<Row> d)
    {
        String url="jdbc:mysql://127.0.0.1:3306/GeorgeDB?";
        Properties prop= new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password", "123456");


       d.write().mode(SaveMode.Overwrite)
               .jdbc(url,"Covid19",prop);
    }
}
