import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureDriver {
	
	//Note: This program should run on cluster,not on local mode as default.
	//When you change any code,make sure you create a new jar out of it.
	public static void main(String[] args) throws Exception {
		
		Path input_dir=new Path("hdfs://localhost:54310/temp_data/");
	    Path output_dir=new Path("hdfs://localhost:54310/output_data");
	    
	    Configuration conf = new Configuration();
	   conf.set("fs.default.name", "hdfs://localhost:54310");
	    conf.set("mapred.job.tracker", "localhost:54311");
	    
	    //create your own jar of the project,store in in local location and specify that location.This is needed for cluster frun
	    conf.set("mapred.jar","/home/hduser/MyWC1.jar");//Create the jar of your own project and specify it here
		   
	    Job job = new Job(conf, "MaxTemp"); //Configuration is not necessary
	   // job.setJobName("MyWordCountJob");
	   
		//Number doesnot depend on split number
	   job.setNumReduceTasks(10);//cause data to be processed by multiple partitioner,no effect on local mode,can b any figure
		
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max temperature");
		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);
		job.setMapperClass(MaxTemperatureMapper.class);
		
		job.setPartitionerClass(MaxTemperaturePartitioner.class);
			
		
		//Reducer code gets executed only at one place,combiner gets executed for all other case
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		  output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
