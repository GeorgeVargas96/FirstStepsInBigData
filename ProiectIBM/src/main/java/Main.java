import org.apache.spark.sql.*;

public class Main {

    public static void main(String []args)
    {

        SparkSession spark=CreareSs.start();
       Procesare p=new Procesare(spark);

       // p.dfFinal().show(50);
        Salvare s=new Salvare();
      // s.salvare(p.dfFinal());
        Citire c=new Citire();
      // c.citire(spark).show(100);
       p.dfFinal(c.citire(spark)).show(100);


        spark.stop();


}

}