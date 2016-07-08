
public class Demo {
public static void main(String[] args) {
	
	String data1=new String("apple");
	String data2=new String("apple");
	
	//a +p +p +l+e *2015
	System.out.println(data1.hashCode());
	System.out.println(data2.hashCode());
	
	int value=data1.hashCode() & Integer.MAX_VALUE;
	
	System.out.println(value%10);
	
	int x=-4567;
	System.out.println(x & Integer.MAX_VALUE);
}
}
