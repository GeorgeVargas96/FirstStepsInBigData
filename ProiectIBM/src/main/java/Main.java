import org.apache.spark.sql.*;

public class Main {

    public static void main(String []args)
    {

        SparkSession spark=BuildSs.start();
        Procesare p=new Procesare(spark);


        ToolDB db=new ToolDB();



        p.procesare().show(true);

        //db.write(p.dfFinal());

        spark.stop();


    }

}