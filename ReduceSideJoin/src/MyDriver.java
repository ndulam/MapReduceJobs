import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Find EmployeeName,companyName with highest  average percentage in Btech and 12th 
public class MyDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Path employeePath=new Path("hdfs://localhost:54310/employee_details/employee.txt");
		Path departmentPath=new Path("hdfs://localhost:54310/employee_details/department.txt");
		
	    Path output_dir=new Path("hdfs://localhost:54310/output_data/");
		
		/*Reads hadoop configuration file,and points to the hadoop cluster*/
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":"); 
		//Create an object of Job by specifying conf object
		Job job = new Job(conf, "ReduceSideJoin");
		 
		//Set your main class in the jar file that will be created in future
	    job.setJarByClass(MyDriver.class);
	   // job.setNumReduceTasks(0);
	    job.setMapperClass(EmployeeMapper.class);
	    job.setMapperClass(DepartmentMapper.class);
	    job.setReducerClass(MyReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	   
	    //Setting your input and output directory
	    //empList1.txt will be processed by MyMapper1
	    MultipleInputs.addInputPath(job, employeePath, TextInputFormat.class, EmployeeMapper.class);
	    //empList2.txt will be procedded by MyMapper2
	    MultipleInputs.addInputPath(job, departmentPath, TextInputFormat.class, DepartmentMapper.class);
	    
	    FileOutputFormat.setOutputPath(job,output_dir );
	    output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
		
	    //This piece of code will actually intiate the Job run
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
