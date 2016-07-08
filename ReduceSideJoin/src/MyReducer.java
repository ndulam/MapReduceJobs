import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	/*
	 * Data will be in this format d005, [”emp,10001,Georgi,M”,"emp,10004,Ravi,M"
	 * ,”dept,Development”]
	 */
	private String empId;
	private String empFName;
	private String gender;
	private String deptName;

	public void reduce(Text deptNo, Iterable<Text> details, Context context)
			throws IOException, InterruptedException {
		System.out.println("MyReducer.reducer()::" + deptNo);
        //Reset this values everytime before starting freshly.Comment this to get wrong answer and debug to find the error
		empId="";
		empFName="";
		gender="";
		deptName="";
		ArrayList <String>empList=new ArrayList<String>();
		
		for (Text detail : details) {
			System.out.println(detail.toString());
			String currentData[] = detail.toString().split(",");
			if (currentData[0].equals("emp")) {
				empId = currentData[1];
				empFName = currentData[2];
				gender = currentData[3];
				String empDetail=empId+"\t"+empFName + "\t" + gender;
				empList.add(empDetail);
			} else if (currentData[0].equals("dept")) {
				deptName = currentData[1];
			}
			
		}
		//Write data only when you have both employee details as well as department details.
		//In our data set we have lot of department info but very few employees.
		
		if(!empList.isEmpty() && deptName.trim().length()!=0)
		{
			for(String emp:empList){
				String empdata[]=emp.split("\t");
				context.write(new Text(empdata[0]), new Text(empdata[1] + "\t" + empdata[2] + "\t"
						+ deptName));
				
			}
		}
	}
}
