package p1;

class Employee {
	private int empId;
	private String empName;

	Employee(int empId, String empName) {
		this.empId = empId;
		this.empName = empName;

	}
	@Override
	public String toString() {
		return "EmpId:"+empId+",EmpName:"+empName;
	}
}

public class StringDemo {
public static void main(String[] args) {
	Employee emp=new Employee(101,"suraj");
	System.out.println(emp);
	System.out.println(emp.toString());
}
}
