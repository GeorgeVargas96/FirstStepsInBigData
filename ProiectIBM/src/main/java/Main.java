import org.apache.spark.sql.*;

public class Main {

    public static void main(String []args)
    {
        BuildSs s=new BuildSs();
        SparkSession spark=s.start();
        Procesare p=new Procesare(spark);


        ToolDB db=new ToolDB();



        p.procesare().show(100);

        //db.write(p.dfFinal());

        spark.stop();


    }

}