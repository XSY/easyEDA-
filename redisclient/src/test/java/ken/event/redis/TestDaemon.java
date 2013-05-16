package ken.event.redis;
/*
 * different try()-while() combination cause different output results.
 * good example for understanding the multi-thread running
 */

public class TestDaemon implements Runnable{
	@Override
	public void run(){
		//try{
			Thread dt = new Thread(new ThreadDaemon());
			dt.setDaemon(true);
			dt.start();
		//	Thread.sleep(10);  //no sleep() call ThreadDaemon don't work. why?
		//} catch(InterruptedException e){
		//	e.printStackTrace();
		//}
		for(int i=0;i<10;i++){
			System.out.println("This is the user thread--"+i);
		}
		
	}
	class ThreadDaemon implements Runnable {
		@Override
		public void run(){
			//while (!Thread.currentThread().isInterrupted()){
				try{
					for(int j=0;j<15;j++)
					System.out.println("This is the daemon thread--"+j);
				    Thread.sleep(100);
					
				}catch(InterruptedException e){
					e.printStackTrace();
				}
				
			//}
		
		}
	}

	public static void main(String...args){
		Thread t = new Thread(new TestDaemon());
		t.start();
	}
}

