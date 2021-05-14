

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
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
        UDF2<BigDecimal,BigDecimal,BigDecimal> newAvg =new UDF2<BigDecimal, BigDecimal, BigDecimal>() {
            @Override
            public BigDecimal call(BigDecimal bigDecimal, BigDecimal bigDecimal2) throws Exception {
                if(bigDecimal==null)return bigDecimal2;
                if(bigDecimal2==null)return bigDecimal;
                return new BigDecimal((bigDecimal.doubleValue()+bigDecimal2.doubleValue())/2);
            }
        };
        spark.udf().register("twoDecimals",twoDecimals,new DecimalType(38,2));
        spark.udf().register("newAvg",newAvg,new DecimalType(38,2));
        return spark;

    }

}
