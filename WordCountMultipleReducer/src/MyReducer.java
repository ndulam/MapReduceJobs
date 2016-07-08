

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	/*arun	1
	david	1
	david	1
	david	1
	david	1
	david	1
	david	1
	dinesh	1
	india	1
	japan	1
	john	1
	john	1
	john	1
	karan	1
	karan	1
	
	kisan	1
	kumar	1*/
	
	/*kiran	1
	kiran	1
	kiran	1
	kiran	1-->  karan, [1,1,1,1]*/
	
	
	
	
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        	System.out.println("reduce(-,-,-)");
        	System.out.println(" context= "+context);
        	
        	System.out.println("key="+key);
        	System.out.print("All values=");
        	
        	
            int sum = 0;
            for (IntWritable value : values) {
            	System.out.print(" "+value.get());
                sum += value.get();
            }
            System.out.println();
            context.write(key, new IntWritable(sum));
        }
    }
