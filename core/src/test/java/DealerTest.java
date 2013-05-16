import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
//import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

public class DealerTest extends Thread {
	public static Logger LOG = Logger.getLogger(DealerTest.class);

	private Context context;
	// private Poller items;
	private Socket socket;
	private byte[] _msgid;

	public DealerTest(byte[] id) {

		_msgid = id;
		context = ZMQ.context(1);
		socket = context.socket(ZMQ.DEALER);
		socket.bind("tcp://*:5560");

		// Initialize poll set
		// items = context.poller(1);
		// items.register(socket, Poller.POLLIN);

	}

	public void run() {

		while (!Thread.currentThread().isInterrupted()) {

			LOG.debug("start sending message!");
			try {
				// socket.send("".getBytes(),ZMQ.SNDMORE);
				socket.send("feeder source".getBytes(), ZMQ.SNDMORE);
				socket.send("".getBytes(), ZMQ.SNDMORE);
				socket.send(_msgid, ZMQ.SNDMORE);
				socket.send("".getBytes(), ZMQ.SNDMORE);
				socket.send("event data".getBytes(), 0);
				LOG.debug("done!waiting reply");

				String m1 = new String(socket.recv(0));
				socket.recv(0);
				String m2 = new String(socket.recv(0));
				LOG.debug("receive reply :    " + m1);
				LOG.debug("receive reply :    " + m2);

				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.error(e);
			}

		}

	}

	public static void main(String[] args) {
		Thread d1 = new DealerTest("msg1".getBytes());
		d1.start();

	}

}
