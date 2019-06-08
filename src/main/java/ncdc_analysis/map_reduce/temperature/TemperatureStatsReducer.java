package ncdc_analysis.map_reduce.temperature;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.IntSummaryStatistics;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TemperatureStatsReducer
        extends Reducer<Text, IntWritable, Text, TemperatureStatsReducer.StatsWriteable> {

    public static class StatsWriteable implements Writable {
        public IntWritable min;
        public IntWritable max;
        public DoubleWritable avg;
        public LongWritable count;

        public StatsWriteable() {
            min = new IntWritable(0);
            max = new IntWritable(0);
            avg = new DoubleWritable(0);
            count = new LongWritable(0);
        }

        public StatsWriteable(IntWritable min, IntWritable max , DoubleWritable avg, LongWritable count) {
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.count = count;
        }

        public void readFields(DataInput in) throws IOException {
            min.readFields(in);
            max.readFields(in);
            avg.readFields(in);
            count.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            min.write(out);
            max.write(out);
            avg.write(out);
            count.write(out);
        }

        @Override
        public String toString() {
            return min + ", " + max + ", " + avg + ", " + count;
        }

        @Override
        public int hashCode() {
            return min.hashCode() * 11 + max.hashCode() * 22 + avg.hashCode() * 33 + count.hashCode() * 44;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StatsWriteable)) {
                return false;
            }
            return this.hashCode() == other.hashCode();
        }
    }


    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        IntSummaryStatistics stats =
                StreamSupport.stream(values.spliterator(), false)
                        .collect(Collectors.summarizingInt(IntWritable::get));

        Integer min = stats.getMin();
        Integer max = stats.getMax();
        Double avg = stats.getAverage();
        Long count = stats.getCount();

        StatsWriteable statsArr = new StatsWriteable(new IntWritable(min), new IntWritable(max), new DoubleWritable(avg), new LongWritable(count));
        context.write(key, statsArr);
    }
}
