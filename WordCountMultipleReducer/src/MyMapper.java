

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
//Sample data
//raja kisan prakash kiran dinesh
	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //Will be called when map slot executes the program
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
    	System.out.println("map(-,-,-)");
    	System.out.println("Key= "+key+" Value= "+value+" Context="+context);
    	
    	String arr[]=value.toString().split(" ");
    	for(String s:arr)
    	{
    		word.set(s);
            context.write(word, one);
    	}
    	    	
       /* StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }*/
    }
	
	
	
	
}
