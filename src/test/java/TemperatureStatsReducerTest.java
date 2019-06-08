import ncdc_analysis.map_reduce.temperature.TemperatureStatsReducer.StatsWriteable;
import ncdc_analysis.map_reduce.temperature.TemperatureStatsReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TemperatureStatsReducerTest {

    @Test
    public void returnsTemperatureStatsInVaues() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, StatsWriteable>()
                .withReducer(new TemperatureStatsReducer())
                .withInput(new Text("1950"),
                           Arrays.asList(new IntWritable(13),
                                         new IntWritable(3),
                                         new IntWritable(2)))
                .withOutput(new Text("1950"),
                            new StatsWriteable(
                                    new IntWritable(2),
                                    new IntWritable(13),
                                    new DoubleWritable(6.0),
                                    new LongWritable(3)))
                .runTest();
    }
}
