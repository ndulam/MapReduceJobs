import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	

	public void reduce(Text empDetails, Iterable<IntWritable> salaries, Context context)
			throws IOException, InterruptedException {
		int total=0;
		
		for (IntWritable salary : salaries) {
			total +=salary.get();
		}
		context.write(empDetails, new IntWritable(total));
	}
}
