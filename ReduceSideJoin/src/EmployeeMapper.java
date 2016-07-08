import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	/*Will Process
	 #[EmpNo,DOB,FName,LName,Gender,HireDate,DeptNo]
	 10001,1953-09-02,Georgi,Facello,M,1986-06-26,d005
	 10002,1964-06-02,Bezalel,Simmel,F,1985-11-21,d007
	 10003,1959-12-03,Parto,Bamford,M,1986-08-28,d004 
	*/
	
	public void map(LongWritable offset, Text value, Context context) throws IOException,
			InterruptedException {
		
		String line = value.toString();
		
		//Skip the first line,as it is just the metadata.
		if(line.startsWith("#")) return ;
		
		System.out.println("EmployeeMapper.map(-,-,-)"+line);
		
		String[] words = line.split(",");
		String empId=words[0].trim();
		String empFName=words[2].trim();
		String gender=words[4].trim();
		String deptNo=words[6].trim();
		
		context.write(new Text(deptNo), new Text("emp,"+empId+","+empFName+","+gender));
	}
}