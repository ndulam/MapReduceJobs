import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxTemperaturePartitioner extends Partitioner<Text, IntWritable> {
	public MaxTemperaturePartitioner() {
		System.out.println("MaxTemperaturePartitioner()");
	}

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		// Default logic of HashPartitioner
		// return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		System.out.println(key.hashCode() + ":" + Integer.MAX_VALUE + ":"
				+ numReduceTasks);

		System.out.println("MaxTemperaturePartioner.getPartition(-,-,-)");

		System.out.println(key + ":: " + value + ":" + numReduceTasks);

		if (Integer.parseInt(key.toString()) % 2 == 0)
			return 0;
		else
			return 1;

	}

}
