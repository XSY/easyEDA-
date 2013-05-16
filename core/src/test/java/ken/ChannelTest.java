package ken;

import java.io.IOException;
//import java.util.Random;

import ken.event.EConfig;
import ken.event.Event;
import ken.event.meta.PatAdmitEvent;
import ken.event.util.JDKSerializeUtil;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

public class ChannelTest extends Thread {
	public static Logger LOG = Logger.getLogger(ChannelTest.class);
	private final static String host = (String) EConfig.loadAll("easyEDA.yaml")
			.get(EConfig.EDA_PIVOT_OUTGOING_HOST);
	private final static int port = ((Integer) EConfig.loadAll("easyEDA.yaml").get(
			EConfig.EDA_PIVOT_OUTGOING_PORT)).intValue();

	private final static int REPLY_SUCCESS = 0;
	// fail, ask to re-send
	private final static int REPLY_FAIL = 1;
	private String dest;
	private ZMQ.Context context;
	private ZMQ.Socket socket;
	private Poller poller;

	public ChannelTest() {

		dest = "tcp://" + host + ":" + port;
		context = ZMQ.context(1);
		socket = context.socket(ZMQ.REP);
		socket.connect(dest);
		LOG.debug("connect sucess!");

		poller = context.poller(1);
		poller.register(socket, Poller.POLLIN);

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void run() {

		int eventcount = 0;

		while (!Thread.currentThread().isInterrupted()) {
			poller.poll();

			if (poller.pollin(0)) {

				try {
					String msgid = new String(socket.recv(0));
					socket.recv(0);
					// LOG.debug("receive msg part :   " + msgid);

					Event<PatAdmitEvent> evt;
					evt = (Event) JDKSerializeUtil.getObject(socket.recv(0));

					socket.send(Integer.toString(REPLY_SUCCESS).getBytes(),
							ZMQ.SNDMORE);
					socket.send("".getBytes(), ZMQ.SNDMORE);
					socket.send(msgid.getBytes(), 0);

					eventcount++;
					LOG.debug("No.[" + eventcount + "] event with type["
							+ evt.getEvtType() + "] is received...");
					LOG.debug("send reply...");
					Thread.sleep(1000);

				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
			// else{
			// LOG.debug("Nothing received, try to connect again...");
			// poller.unregister(socket);
			// socket.setLinger(0);
			// socket.close();
			// socket = context.socket(ZMQ.REP);
			// socket.connect(dest);
			// poller = context.poller(1);
			// poller.register(socket, Poller.POLLIN);
			// }

		}
		socket.close();
		context.term();

	}

	public static void main(String[] args) {
		Thread channel = new ChannelTest();
		channel.start();

	}

}
