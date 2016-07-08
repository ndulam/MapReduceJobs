import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Student implements Writable {
	private Text studentName;
	private Text schoolName;
	private Text cityName;
	private IntWritable marks;

	public Student() {
		System.out.println("Student()");
		this.studentName = new Text();
		this.schoolName = new Text();
		this.cityName = new Text();
		this.marks = new IntWritable();
	}

	public Student(Text studentName, IntWritable marks, Text schoolName,
			Text cityName) {
		System.out.println("Student(-,-,-,-)");
		this.studentName = studentName;
		this.schoolName = schoolName;
		this.cityName = cityName;
		this.marks = marks;
	}

	// deep copy constructor
	public Student(Student st) {
		this.studentName = new Text(st.studentName);
		this.schoolName = new Text(st.schoolName);
		this.cityName = new Text(st.cityName);
		this.marks = new IntWritable(st.marks.get());
	}

	/*public String toString() {
		return studentName + " " + schoolName;
	}*/

	public Text getStudentName() {
		System.out.println("Student.getStudentName()");
		return studentName;
	}

	@Override
	public String toString() {
		return "Student [studentName=" + studentName + ", schoolName="
				+ schoolName + ", cityName=" + cityName + ", marks=" + marks
				+ "]";
	}

	public Text getSchoolName() {
		System.out.println("Student.getSchoolName()");
		return schoolName;
	}

	public Text getCityName() {
		System.out.println("Student.getCityName()");
		return cityName;
	}

	public IntWritable getMarks() {
		System.out.println("student.getMarks()");
		return marks;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		System.out.println("Student.write(-)");
		studentName.write(out);
		schoolName.write(out);
		cityName.write(out);
		marks.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		System.out.println("Student.readFields(-)");
		studentName.readFields(in);
		schoolName.readFields(in);
		cityName.readFields(in);
		marks.readFields(in);
	}

	@Override
	public int hashCode() {
		return marks.get() + studentName.hashCode();
	}
}
