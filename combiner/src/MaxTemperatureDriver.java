import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureDriver {
	public static void main(String[] args) throws Exception {
		
		Path input_dir=new Path("hdfs://localhost:54310/temp_data/");
	    Path output_dir=new Path("hdfs://localhost:54310/tmp_output/");
		
		Job job = new Job();
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max temperature");
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureCombiner.class);//you can use same reducer code as combiner.
		//Reducer code gets executed only at one place,combiner gets executed for all other case
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);
		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
