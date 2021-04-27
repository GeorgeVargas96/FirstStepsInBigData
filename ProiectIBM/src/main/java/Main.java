import org.apache.spark.sql.*;

public class Main {

    public static void main(String []args)
    {

        SparkSession spark=CreareSs.start();
        Procesare p=new Procesare(spark);
        p.afisare();
        p.dfFinal2().show();
        p.dfFinal().show(50);
        Salvare.salvare(p.dfFinal2());


        spark.stop();


}

}