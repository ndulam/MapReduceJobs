import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	
	public MaxTemperatureMapper() {
		System.out.println("MaxTemperatureMapper()");
	}

	/*
	1901 34 
	1903 71
	1901 20
	1903 72
	*/
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String data[] = value.toString().split(" ");
		System.out.println("MaxTemperatureMapper.map(-,-,-):"+data[0]+" "+data[1]);
		context.write(new Text(data[0] + " " + data[1]), NullWritable.get());
	}
}
