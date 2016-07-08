
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {
	public MaxTemperatureReducer(){
		System.out.println("MaxTemperatureReducer()");
	}
	@Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureReducer.reduce(-,-,-)");
		System.out.println("Key="+key);
		
		context.write(key, NullWritable.get());
	}
}
