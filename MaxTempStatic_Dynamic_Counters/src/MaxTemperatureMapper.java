import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
enum Temperature{
TOTAL_PROCESSED,
GREATER_THAN_10;

}

public class MaxTemperatureMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {
	  private static final int MISSING = 9999;
	  @Override  public void map(LongWritable key, Text value, Context context)      throws IOException, InterruptedException 
	  {
		  
		  context.getCounter("Temperature", "TOTAL_PROCESSED").increment(1);//use -1 to decrease
		  
		  String line = value.toString(); 
		  String year = line.substring(15, 19); 
		  int airTemperature; 
		  if (line.charAt(87) == '+') 
		  { 
			  // parseInt doesn't like leading plus signs   
			  airTemperature = Integer.parseInt(line.substring(88, 92));  
			  } else {      airTemperature = Integer.parseInt(line.substring(87, 92));
			  }  
		  String quality = line.substring(92, 93);   
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
			//Dynamic Counter
			context.getCounter("Different Years",new Text(year).toString()).increment(1);
			
			if (airTemperature/10 > 10)
				context.getCounter("Temperature", "GREATER_THAN_10").increment(1);
		}
	}
}
	 