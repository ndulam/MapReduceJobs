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
		System.out.println("Default sorting Algorithm"+job.getConfiguration().get("map.sort.class"));
		//job.getConfiguration().set("map.sort.class","org.apache.hadoop.tools.MergeSort");
		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setNumReduceTasks(1);
		System.out.println("Default Comparator: "+job.getSortComparator());
		
		job.setSortComparatorClass(MyKeyComparator.class);
		
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		  output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
