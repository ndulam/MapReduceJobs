

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<IntWritable, Student, IntWritable, Student> {
	MultipleOutputs mos=null;
	ArrayList<Student> topScorers;
	int topScore=0;
	protected void setup(Context context) throws IOException ,InterruptedException {
		mos=new MultipleOutputs(context);
		topScorers=new ArrayList<Student>();
		System.out.println("MyReducer.setup(-,-)");
	};
	
	
	/*900 student1
	  900 student2,  
	  900 student3..... 
	  
	  900 [student1,student2,student3]*/
	
	@Override
    public void reduce(IntWritable currentMark, Iterable<Student> values, Context context)
        throws IOException, InterruptedException {
    	
    	System.out.println("key="+currentMark);
    	System.out.print("All values=");
    	
    	
    	if(currentMark.get()>topScore){
    		topScore=currentMark.get();
    		topScorers.clear();//clear all element from arrayList
    		
    		for(Student st:values){	
        		System.out.println("topScorer:"+st);
        		
    				// Dont directly add object to the List,Map-reduce will copy all
    				// same instance to the Object and we certainly dont need the
    				// dupicate object
    				// use deep copy constructor concept and create new object and
    				// then store it
    				// dont get fooled by the size
    				Student st1=new Student(st);
        		
        	   	topScorers.add(st1);
        	}
    	}
    }
   
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		System.out.println("MyReducer.cleanup(-)");
		
		for (Student st : topScorers) {
			System.out.println(st);
			//Use mos to store result in resultant file
			mos.write(st.getMarks(), st, "top-Scorer.txt");
															
		}
		mos.close();
	}
}