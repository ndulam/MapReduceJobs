import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
	protected MyGroupComparator() {
		super(Text.class,true);//second parameter will start the buffer,if you dont provide this value,it throws NullPOinterException
			System.out.println("MyGroupComparator()");
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		System.out.println("MyGroupComparator.compare(-,-)::"+a+" : "+b);
//1901 23
//1901 32
//should fall on same group.ie only year should be used.		
		String yearTemp1[] = ((Text) a).toString().split(" ");
		String yearTemp2[] = ((Text) b).toString().split(" ");

		int year1 = Integer.parseInt(yearTemp1[0]);

		int year2 = Integer.parseInt(yearTemp2[0]);
//+ve means heavier
//-ve means lighter
		return year1 - year2;// 1901-1905
	}
}
