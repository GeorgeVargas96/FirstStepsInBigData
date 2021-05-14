
import java.sql.SQLSyntaxErrorException;
import java.util.Properties;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
public class Citire {
    String url="jdbc:mysql://127.0.0.1:3306/GeorgeDB?";
    Properties prop= new Properties();

    { prop.setProperty("user","root");
        prop.setProperty("password", "123456");
    }

    public Dataset<Row> citire(SparkSession spark)
    {
        return spark.read().jdbc(url, "Covid19", prop);



    }
}
