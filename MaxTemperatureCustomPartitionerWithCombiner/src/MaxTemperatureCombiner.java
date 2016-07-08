import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureCombiner extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	public MaxTemperatureCombiner() {
		System.out.println("MaxTemperatureCombiner()");
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureCombiner.reduce(-,-,-)");
		System.out.println("Key=" + key);
		System.out.println("Values...");
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			System.out.println("" + value.get());
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}
