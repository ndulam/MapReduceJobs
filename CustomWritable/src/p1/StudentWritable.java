package p1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StudentWritable implements Writable {
	private Text studentName;
	private IntWritable studentMark;
	private Text schoolName;
	private Text stateName;
	private Text emailId;
	
	public StudentWritable() {
		studentName=new Text();
		studentMark=new IntWritable();
		schoolName=new Text();
		stateName=new Text();
		emailId=new Text();
	}
	
	
	public StudentWritable(Text studentName, IntWritable studentMark,
			Text schoolName, Text stateName, Text emailId) {
		this.studentName = studentName;
		this.studentMark = studentMark;
		this.schoolName = schoolName;
		this.stateName = stateName;
		this.emailId = emailId;
	}
	public Text getStudentName() {
		return studentName;
	}
	public void setStudentName(Text studentName) {
		this.studentName = studentName;
	}
	public IntWritable getStudentMark() {
		return studentMark;
	}
	public void setStudentMark(IntWritable studentMark) {
		this.studentMark = studentMark;
	}
	public Text getSchoolName() {
		return schoolName;
	}
	public void setSchoolName(Text schoolName) {
		this.schoolName = schoolName;
	}
	public Text getStateName() {
		return stateName;
	}
	public void setStateName(Text stateName) {
		this.stateName = stateName;
	}
	public Text getEmailId() {
		return emailId;
	}
	public void setEmailId(Text emailId) {
		this.emailId = emailId;
	}
   
	@Override //From Writable
	public void readFields(DataInput in) throws IOException {
		System.out.println("StudentWriable.readFields(-)");
		studentName.readFields(in);	
		studentMark.readFields(in);
		schoolName.readFields(in);
		stateName.readFields(in);
		emailId.readFields(in);
	}


	@Override //From Writable
	public void write(DataOutput out) throws IOException {
		System.out.println("StudentWritable.write(-)");
		studentName.write(out);
		studentMark.write(out);
		schoolName.write(out);
		stateName.write(out);
		emailId.write(out);
		
	}
	
	
}
