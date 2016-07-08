
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxTemperaturePartitioner extends 	Partitioner<Text, NullWritable>
{
	public MaxTemperaturePartitioner(){
		System.out.println("MaxTemperaturePartitioner()");
	}
	//1901 32
	@Override
	public int getPartition(Text key, NullWritable value, int numPartitions) {
		System.out.println("MaxTemperaturePartitioner.getPartition(-,-,-)");
	int year=Integer.parseInt(key.toString().split(" ")[0]); 
	return year%numPartitions;//4=> 0 1 2 3
	}
}
