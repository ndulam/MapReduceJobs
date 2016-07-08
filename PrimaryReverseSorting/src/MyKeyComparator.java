import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyKeyComparator  extends WritableComparator{
//Super class doesnot have default constructor
	protected MyKeyComparator() {
		super(Text.class,true);//second parameter will start the buffer,if you dont provide this value,it throws NullPOinterException
		System.out.println("MyKeyComparator()");
		
	}
	@Override
		public int compare(WritableComparable a, WritableComparable b) {
		System.out.println("MyKeyComparator.compare(-,-)");
		System.out.println(a+"<=>"+b);
		int year1=Integer.parseInt(((Text)a).toString());
		int year2=Integer.parseInt(((Text)b).toString());
		
		return -(year1-year2);
		
		//return (year1-year2);//default Implementation
		}
}
	 