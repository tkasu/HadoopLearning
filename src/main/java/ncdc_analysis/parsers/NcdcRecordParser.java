package ncdc_analysis.parsers;

import org.apache.hadoop.io.Text;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class NcdcRecordParser {

    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;

    public static Dataset<Row> parse(Dataset<String> dataset) {

        // TODO For some reason substring start positions need to be shifted by +1, investigate why?
        Dataset<Row> df = dataset.select(
                dataset.col("value").substr(16, 4).cast("int").alias("year"),
                dataset.col("value").substr(88, 5).cast("int").alias("temperature")
        );

        // Filter out missing temperatures
        df = df.filter(col("temperature").notEqual(lit(MISSING_TEMPERATURE)));

        // Temps are int by default where last digit is the first decimal, e.g. 102 -> 10.2C
        df = df.withColumn("temperature", col("temperature").cast("float").divide(lit(10)));

        return df;
    }

    public void parse(String record) {
        year = record.substring(15, 19);
        String airTemperatureString;
        // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }
}