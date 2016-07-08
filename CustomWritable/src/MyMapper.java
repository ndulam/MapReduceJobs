

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, Student> {
        
	MyMapper(){
		System.out.println("MyMapper()");
	}
  //mahesh,980,SMCS,Orissa,mahesh@gmail.com
	//0     1    2    3		4
     //called once for each record.
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
      {
    	System.out.println("MyMapper().map(-,-,-)");
    	//each record contains data in mahesh,980,SMCS,Orissa,mahesh@gmail.com format
    	String arr[]=value.toString().split(",");
    	
    	Text studentName=new Text(arr[0]);
    	IntWritable marks=new IntWritable(Integer.parseInt(arr[1]));
    	Text schoolName=new Text(arr[2]);
    	Text state=new Text(arr[3]);
    	
    	System.out.println("Marks="+marks);
    	Student student=new Student(studentName,marks,schoolName,state);    	
    	
        context.write(marks, student);//key we are using same,so that they can be grouped together
        System.out.println("Mapper Ouput:"+marks+" "+student);
        }
}