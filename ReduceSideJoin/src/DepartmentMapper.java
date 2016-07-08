import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//Will Process
/*
#[dept_no,dept_name]
d009,Customer Service
d005,Development
d002,Finance
d003,Human Resources
d001,Marketing
d004,Production
d006,Quality Management
d008,Research
d007,Sales
*/
public class DepartmentMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable offset, Text value, Context con) throws IOException,
			InterruptedException {
		
		String line = value.toString();
		if(line.startsWith("#")) return;
		
		System.out.println("DepartmentMapper.map(-,-,-)"+line);
		
		String[] words = line.split(",");
		String deptNo = words[0].trim();
		String deptName=words[1].trim();
		
		con.write(new Text(deptNo), new Text("dept,"+deptName));
	}
}