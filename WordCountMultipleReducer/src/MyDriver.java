
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyDriver {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// If cluster is not running,it will give retrying server... error on the console
		Path input_dir = new Path("hdfs://localhost:54310/input_data/");
		Path output_dir = new Path("hdfs://localhost:54310/output_data/");

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:54310");
		conf.set("mapred.job.tracker", "localhost:54311");

		//create a jar of this project and specify the path here.It is to run the project in pseudo-distributed mode.
		conf.set("mapred.jar", "/home/hduser/MyWC1.jar");
														
		Job job = new Job(conf, "MyWordCountJob"); 
		
		job.setNumReduceTasks(10);// causes data to be processed by multiple Reducer,no effect on local mode.
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(MyDriver.class);
		
		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);

		// setting output key type
		job.setOutputKeyClass(Text.class);
		// if you dont provide this,it assumes Text,and hence error
		job.setOutputValueClass(IntWritable.class);

		// Setting your input and output directory
		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);

		// This piece of code will actually initiate the Job run
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
