public class test {

	private  String dest;

	// public test(){
	//
	// }

	public void change() {
		dest = "hello!";

	}
	
	public void secChange(){
		
		print();
		dest = "world!";
	}
	public void print(){
		System.out.println(dest);
	}

	public static void main(String[] args) {
		test tt = new test();
		tt.change();
		tt.secChange();
		tt.print();
		
	

	}
}
