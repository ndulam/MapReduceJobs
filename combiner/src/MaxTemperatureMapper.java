import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	public MaxTemperatureMapper() {
		System.out.println("MaxTemperatureMapper()");
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("mapper.map(-,-,-)");
		String data[] = value.toString().split(" ");

		context.write(new Text(data[0]),
				new IntWritable(Integer.parseInt(data[1])));
	}
}
