import org.apache.spark.sql.*;

public class Main {

    public static void main(String []args)
    {
        System.setProperty("hadoop.home.dir", System.getProperty("user.home"));


        Procesare p=new Procesare();


        ToolDB db=new ToolDB();



        p.procesare().show(100);

        db.write(p.procesare());

        p.getSpark().stop();


    }

}