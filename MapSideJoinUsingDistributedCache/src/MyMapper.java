import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	HashMap<String,String> deptMap=new HashMap<String,String>();
	HashMap<String,String> empMap=new HashMap<String,String>();
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		/*
		 * Default size of Distributed Cache is 10 GB,We can control the size of
		 * the distributed cache by explicitly defining its size in hadoop’s
		 * configuration file local.cache.size.
		 */
		/*
		 * Thus, Distributed cache is a mechanism to caching readonly data over
		 * Hadoop cluster. The sending of readOnly files occurs at the time of
		 * job creation and the framework makes the cached files available to
		 * the cluster nodes at their computational time.
		 */

		System.out.println("MaxTemperatureMapper.setUp(-,-,-,-)");
		Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		System.out.println("Total Files =" + uris.length);

		for (int i = 0; i < uris.length; i++) {
			String fileName = uris[i].toString();
			Scanner scan = new Scanner(new FileReader(fileName));
				
				while (scan.hasNext()) {
					String line = scan.nextLine().toString();
					
					if (line.startsWith("#")){
						continue; // for first line
					}

					String data[] = line.split(",");
					if (fileName.contains("department.txt")){
						System.out.println("department::"+data[0]+"::"+data[1]);
					deptMap.put(data[0].trim(), data[1].trim());
					}
					else if(fileName.contains("employee.txt")){
						//10001,1953-09-02,Georgi,Facello,M,1986-06-26,d005
						empMap.put(data[0].trim(), data[2]+","+data[4]+","+data[6]);
					}

				}
				scan.close();
			
			
			// do whatever you like with xml using parser
		}

	}
	public void map(LongWritable offset, Text value, Context con) throws IOException,
			InterruptedException {
		//History salary data 
		//10001,88958,2002-06-22,9999-01-01
		String line = value.toString();
		if(line.startsWith("#")) return;
		
		System.out.println("DepartmentMapper.map(-,-,-)"+line);
		
		String[] words = line.split(",");
		String empId = words[0].trim();
		int salary=Integer.parseInt(words[1].trim());
		//based on empId find deptId from empMap
		String empDetails=empMap.get(empId);
		//empDetails=  "Georgi,M,d005")
		String empData[]=empDetails.split(",");
		String deptId=empData[2].trim();
		
		//based on deptId find deptName from deptMap
		String deptName=deptMap.get(deptId);
		//deptName=Development
		con.write(new Text(empId+","+empData[0]+","+empData[1]+","+deptName), new IntWritable(salary));
		//con.write(new Text(deptNo), new Text("dept,"+deptName));
	}
}