import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQ.Poller;

public class RepTest extends Thread {
	public static Logger LOG = Logger.getLogger(RepTest.class);
	private Context context;
	private Poller items;
	private Socket socket;

	// private byte[] _msgid;

	public RepTest() {

		// _msgid = id;
		context = ZMQ.context(1);
		socket = context.socket(ZMQ.REP);

		// Initialize poll set
		items = context.poller(1);
		items.register(socket, Poller.POLLIN);

	}

	public void run() {

		socket.connect("tcp://*:" + "5560");
		LOG.debug("start recieving message...");

		while (!Thread.currentThread().isInterrupted()) {

			items.poll();
			if (items.pollin(0)) {
				String m1 = new String(socket.recv(0));
				socket.recv(0);
				String m2 = new String(socket.recv(0));
				// socket.recv(0);
				// String m3= new String(socket.recv(0));
				LOG.debug("receive msg part :   " + m1);
				LOG.debug("receive msg part :   " + m2);
				// LOG.debug("receive msg part :   " + m3);

				socket.send(m1.getBytes(), 0);
			}

		}
		socket.close();
		context.term();
	}

	public static void main(String[] args) {
		Thread d1 = new RepTest();
		d1.start();

	}

}
