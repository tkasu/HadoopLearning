
import ncdc_analysis.parsers.NcdcRecordParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;

public class NdcdDataframeParserTest {
    private SparkSession spark;

    private void initSparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("NCDC MaxTemperature")
                .master("local")
                .getOrCreate();
        this.spark = spark;
    }

    @Test
    public void processesValidRecord() {
        initSparkSession();

        String ncdcRecord = "0043011990999991950051518004+68750+023550FM-12+0382" +
                                    // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999";
                                       // Temperature ^^^^^
        Dataset<String> dataset =
            spark.createDataset(Arrays.asList(
                    ncdcRecord
            ), Encoders.STRING());

        Dataset<Row> df = NcdcRecordParser.parse(dataset);
        Integer year = (Integer) df.select(col("year")).first().getInt(0);
        Double temp = (Double) df.select(col("temperature")).first().getDouble(0);
        assertEquals(new Integer(1950), year);
        assertEquals(new Double(-1.1), temp);

    }

    @Test
    public void ignoreMissingTemps() {
        initSparkSession();

        String ncdcRecord = "0043011990999991950051518004+68750+023550FM-12+0382" +
                                    // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999";
                              // Temperature ^^^^^
        Dataset<String> dataset =
                spark.createDataset(Arrays.asList(
                        ncdcRecord
                ), Encoders.STRING());

        Dataset<Row> df = NcdcRecordParser.parse(dataset);
        assertEquals(true, df.isEmpty());

    }
}

