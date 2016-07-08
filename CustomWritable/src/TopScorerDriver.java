import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopScorerDriver {
	// inputdata contains students details
	// studentname,studentmarks,student_schoolname,city
	// We need to find out the highest mark as well as the details of the
	// student
	// There are two ways,in the value as singlekey,you append all the
	// strings,or use a Custom class implementing Writable that can act as a
	// Value
	public static void main(String[] args) throws Exception {

		Path input_dir = new Path("hdfs://localhost:54310/student_marks");
		Path output_dir = new Path("hdfs://localhost:54310/output_data");

		Configuration conf = new Configuration();

		Job job = new Job(conf, "MyWordCountJob");
		job.setJarByClass(TopScorerDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// delete the output files at every run
		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,
				true);

		// specifying reducers output value
		// if you dont provide this,it assumes LongWritable,and then error
		job.setOutputKeyClass(IntWritable.class);
		// if you dont provide this,it assumes Text,and hence error
		job.setOutputValueClass(Student.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Student.class);
		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);

		// This piece of code will actually intiate the Job run
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		// By default a folder is create output_data with 2 files _SUCCESS and
		// part-r-00000
	}
}
