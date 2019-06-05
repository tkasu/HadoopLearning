package ncdc_analysis.map_reduce.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        Optional<Integer> maxValue = StreamSupport.stream(values.spliterator(), false)
                .map(IntWritable::get)
                .max(Integer::compareTo);

        if (maxValue.isPresent()) {
            context.write(key, new IntWritable(maxValue.get()));
        }
    }
}
