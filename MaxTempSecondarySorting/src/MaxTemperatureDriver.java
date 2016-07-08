import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureDriver {
	public static void main(String[] args) throws Exception {
		
		Path input_dir=new Path("hdfs://localhost:54310/temp_data/");
	    Path output_dir=new Path("hdfs://localhost:54310/tmp_output/");
				
		Job job = new Job();
		System.out.println("Default Grouping Comparator: "+job.getGroupingComparator());

		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max temperature");
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setPartitionerClass(MaxTemperaturePartitioner.class);
		job.setSortComparatorClass(MyKeyComparator.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
