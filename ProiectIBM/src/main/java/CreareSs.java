

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DecimalType;

import java.math.BigDecimal;


public class CreareSs {

    public static SparkSession start()
    {
        System.setProperty("hadoop.home.dir", System.getProperty("user.home"));

        SparkSession spark=SparkSession.builder()
                .appName("BigData")
                .config("spark.master","local[*]").getOrCreate();

       UDF1<Double, BigDecimal> twoDecimals=new UDF1<Double, BigDecimal>() {
           @Override
           public BigDecimal call(Double aDouble) throws Exception {



               return new BigDecimal(aDouble.toString());
           }
       };
       spark.udf().register("twoDecimals",twoDecimals,new DecimalType(10,2));
        return spark;

    }

}
