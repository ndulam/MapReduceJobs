import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Find EmployeeName,companyName with highest  average percentage in Btech and 12th 
public class MyDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		Path input_dir=new Path("hdfs://localhost:54310/employee_details/HistorySalary.txt");
		
	    Path output_dir=new Path("hdfs://localhost:54310/output_data/");
	    
		/*Reads hadoop configuration file,and points to the hadoop cluster*/
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":"); 
		conf.set("fs.default.name", "hdfs://localhost:54310");
	    conf.set("mapred.job.tracker", "localhost:54311");
	    
	    //create your own jar of the project,store in in local location and specify that location.This is needed for cluster frun
	    conf.set("mapred.jar","/home/hduser/MapSideJoin.jar");//Create the jar of your own project and specify it here
		
	    DistributedCache.addCacheFile(new URI("/employee_details/department.txt"), conf);
		DistributedCache.addCacheFile(new URI("/employee_details/employee.txt"), conf);
		
		//Create an object of Job by specifying conf object
		Job job = new Job(conf, "ReduceSideJoin");
		// job.setNumReduceTasks(0);
		//Set your main class in the jar file that will be created in future
	    job.setJarByClass(MyDriver.class);
	   
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, input_dir);        
	    FileOutputFormat.setOutputPath(job,output_dir );
	    output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
		
	    //This piece of code will actually intiate the Job run
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
