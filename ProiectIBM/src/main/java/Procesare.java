import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;


import static java.util.Arrays.asList;

import static org.apache.spark.sql.functions.*;

public class Procesare {
    private final SparkSession s;
    private final Dataset<Row> data;

    private final static String elementaryCases = "Elementary school cases";
    private final static String middleCases = "Middle school cases";
    private final static String highCases = "High school cases";
    private final static StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("School unit name", DataTypes.StringType, false),
            DataTypes.createStructField("Elementary school cases", DataTypes.IntegerType, true),
            DataTypes.createStructField("Middle school cases", DataTypes.IntegerType, true),
            DataTypes.createStructField("High school cases", DataTypes.IntegerType, true),
            DataTypes.createStructField("Gender", DataTypes.StringType, true),
            DataTypes.createStructField("Reporting date", DataTypes.DateType, true)
    });

    public Procesare(SparkSession s) {
        this.s = s;
        data = this.s.read().format("csv")
                .option("header", "true").schema(schema)
                .load("ProiectIBM/data2.csv");

    }



    public Dataset<Row> tempDF(String numeCol) {
        return data.groupBy(col("School unit name"), col("Gender"))
                .agg(avg(numeCol).as("Old"))
                .withColumn(numeCol,callUDF("twoDecimals",col("Old")));
    }

    public Dataset<Row> dfFinal() {

        Dataset<Row> df1 = tempDF(elementaryCases).drop("Old");
        Dataset<Row> df2 = tempDF(middleCases).drop("Old");
        Dataset<Row> df3 = tempDF(highCases).drop("Old");
        return df1.join(df2, JavaConverters.asScalaBuffer(asList("School unit name", "Gender")),"full")
                .join(df3, JavaConverters.asScalaBuffer(asList("School unit name", "Gender")),"full");


    }


    public Dataset<Row> dfFinal (Dataset<Row> dfSql)
    {

        Dataset<Row> dfCsv=dfFinal();

        return dfSql.join(dfCsv,JavaConverters.asScalaBuffer(asList("School unit name", "Gender")),"full")
                .withColumn("elementaryCases",callUDF("newAvg",dfSql.col(elementaryCases),dfCsv.col(elementaryCases)))
                .withColumn("middleCases",callUDF("newAvg",dfSql.col(middleCases),dfCsv.col(middleCases)))
                .withColumn("highCases",callUDF("newAvg",dfSql.col(highCases),dfCsv.col(highCases)))
                .drop(JavaConverters.asScalaBuffer(asList(elementaryCases,middleCases,highCases)))
                .withColumnRenamed("elementaryCases",elementaryCases)
                .withColumnRenamed("middleCases",middleCases)
                .withColumnRenamed("highCases",highCases);
    }

    public Dataset<Row> procesare()
    {
        ToolDB db=new ToolDB();
        Dataset<Row> dfSql= db.read(s);
        if(dfSql.isEmpty())return dfFinal();
        else return dfFinal(dfSql);
    }



}
