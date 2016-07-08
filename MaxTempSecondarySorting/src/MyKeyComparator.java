import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyKeyComparator extends 	WritableComparator
{
	protected MyKeyComparator() {
		super(Text.class,true);//second parameter will start the buffer,if you dont provide this value,it throws NullPOinterException
		System.out.println("MyKeyComparator()");
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//a-->1901 26
		//b-->1901 34
		System.out.println("MyKeyComparator.compare(-,-) ::"+a+": "+b);
		String yearTemp1[]=((Text)a).toString().split(" ");
		String yearTemp2[]=((Text)b).toString().split(" ");
		
		int year1=Integer.parseInt(yearTemp1[0]);
		int temp1=Integer.parseInt(yearTemp1[1]);
		
		int year2=Integer.parseInt(yearTemp2[0]);
		int temp2=Integer.parseInt(yearTemp2[1]);
		
		//if two years are not equal,send +ve or -ve 
		//but if they are equal check their temperature and send +ve or -ve
		
		int cmp=year1-year2;
		if (cmp != 0) {
			return cmp;
		}
		
		//if both Years are equal compare the temperature
		else{
			return -(temp1-temp2);//highest temperature should come first
		}
	//Text
	}
}
