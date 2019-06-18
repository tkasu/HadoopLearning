package ncdc_analysis.spark.temperature;

import ncdc_analysis.parsers.NcdcRecordParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MaxTemperatureApp {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("MaxTemperatureApp Usage: <input> <output>\n");
            return;
        }
        MaxTemperatureApp app = new MaxTemperatureApp();
        app.start(args[0], args[1]);
    }

    private void start(String input, String output) {

        // Requires --master to be set in spark-submit
        // In development, you can add following flag to your run configuration's VM Options:
        // -Dspark.master=local[*]
        SparkSession spark = SparkSession.builder()
                .appName("NCDC MaxTemperature")
                .getOrCreate();

        Dataset<String> dataset = spark.read().textFile(input);

        Dataset<Row> df = NcdcRecordParser.parse(dataset);
        df.repartition(8);
        df.createOrReplaceTempView("ncdc_temperature");

        Dataset<Row> resultDf =
            spark.sql("SELECT " +
                      "  year, max(temperature) as max_temp " +
                      "FROM " +
                      "  ncdc_temperature " +
                      "GROUP BY " +
                      "  year " +
                      "ORDER BY" +
                      "  year");

        resultDf.repartition(1)
                .write().format("com.databricks.spark.csv")
                .option("header", true)
                .mode("overwrite")
                .save(output);
        }
}