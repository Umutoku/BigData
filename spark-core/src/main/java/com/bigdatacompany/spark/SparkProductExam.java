package com.bigdatacompany.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkProductExam {
    public static void main(String[] args) throws AnalysisException {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType()
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("country", DataTypes.StringType)
                .add("product",DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();

        Dataset<Row> rawDS = sparkSession.read()
                .schema(schema).option("multiline",true)
                .json("C:\\Users\\umuto\\OneDrive\\Masaüstü\\product.json");

        Dataset<Row> sumPriceDS = rawDS.groupBy("country","product").sum("price");
        Dataset<Row> countPriceDS = rawDS.groupBy("country","product").count();
        //sum yerine count gibi sayma işlemi için alternatif kullanılabilir

        //countPriceDS.sort(functions.desc("count")).show();

        //countPriceDS.show();


        //SQL sorguları için yapılması gereken
        rawDS.createOrReplaceTempView("product");
        rawDS.createGlobalTempView("product");
        //birisi cluster üzerinde oluştururken diğeri sadece section bazlı oluşur

        Dataset<Row> sqlDS = sparkSession.sql("select * from product where price > 15");
        sqlDS.show();
    }
}
