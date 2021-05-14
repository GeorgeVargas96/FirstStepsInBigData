import org.apache.spark.sql.*;

import java.sql.SQLSyntaxErrorException;

public class Main {

    public static void main(String []args)
    {

        SparkSession spark=CreareSs.start();
        Procesare p=new Procesare(spark);

        // p.dfFinal().show(50);
        ToolDB db=new ToolDB();

        // p.dfFinal().show();

        p.dfFinal(db.read(spark)).show(100);

        //db.write(p.dfFinal());

        spark.stop();


    }

}