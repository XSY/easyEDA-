package ken.event.redis;


public class Test extends Thread {
    public Test() { 
        
    } 
    
    public void run() { 
    	while(!Thread.currentThread().isInterrupted()){
    		 for (int i = 0 ; i < 10 ; i ++) { 
    	            try {
    	                Thread.sleep(100) ;
    	            } catch (InterruptedException e) {
    	                // TODO Auto-generated catch block
    	                e.printStackTrace();
    	            } 
    	            System.out.println(i); 
    	            if(Thread.currentThread().isDaemon())
    	            	System.out.println("Daemon thread.");
    	        }
    	}
       
    } 	
            
    public static void main (String args[]) { 
    	Test tt = new Test();
    	tt.start();
        Test test = new Test() ; 
        test.setDaemon(true) ; 
        test.start() ; 
        
        System.out.println("isDaemon=" + test.isDaemon()); 
        
    }
}