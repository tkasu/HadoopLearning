package ncdc_analysis.map_reduce.temperature;

import ncdc_analysis.parsers.NcdcRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Example is from Hadoop: The Definete Guide http://hadoopbook.com/
public class YearTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()),
                          new IntWritable(parser.getAirTemperature()));
        }
    }
}
