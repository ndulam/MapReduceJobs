import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	public MaxTemperatureReducer() {
		System.out.println("MaxTemperatureReducer");
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.print("Reducer.reduce(-,-,-):"+key);
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		System.out.println("\t Max Temp:"+maxValue);
		context.write(key, new IntWritable(maxValue));
	}
}
