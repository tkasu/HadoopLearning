import ncdc_analysis.map_reduce.temperature.MaxTemperatureReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

import java.io.IOException;
import java.util.Arrays;

public class MaxTemperatureReducerTest {

    @Test
    public void returnsMaximumIntegerInVaues() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1950"),
                           Arrays.asList(new IntWritable(10),
                                         new IntWritable(5)))
                .withOutput(new Text("1950"), new IntWritable(10))
                .runTest();
    }
}
