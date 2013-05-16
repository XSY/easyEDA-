package ken.event.client.feeder;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

import ken.event.EConfig;
import ken.event.Event;
import ken.event.HistoryWork;
import ken.event.Swap;
import ken.event.WaitingEvent;
import ken.event.meta.AtomicE;
import ken.event.util.EventBuilder;
import ken.event.util.FileUtil;
import ken.event.util.JDKSerializeUtil;

import ken.event.client.QueuedEventBox;
import ken.event.client.adapter.IAdapter;
import ken.event.client.feeder.IFeeder;

/**
 * @author KennyZJ
 * 
 */
@SuppressWarnings("rawtypes")
public class BasicFeeder extends Thread implements IFeeder {

	public static Logger LOG = Logger.getLogger(BasicFeeder.class);

	public final static String EXT = ".evt";
	public final static String DIR_ALL = "waiting";
	public final static String DIR_DONE = "done";
	public final static String DIR_FORK = "fork-history";

	private final static Map<String, Object> conf = EConfig
			.loadAll("easyEDA.yaml");

	private String dirhome;
	// the feeder name
	private String _name;
	// socket destination
	private String _dest;
	private final static String final_dest = "channel";

	private QueuedEventBox eventbox;
	private List<IAdapter> adapters;

	private LinkedBlockingQueue<String> done;
	private LinkedBlockingQueue<Event> all;
	private LinkedBlockingQueue<Event> pending;
	private ConcurrentHashMap<String, String> flag;
	private ConcurrentHashMap<String, WaitingEvent> waitingReply;

	private ZMQ.Context context;
	private ZMQ.Socket socket;
	private ZMQ.Poller poller;

	private int swapall_count;
	private int swapdone_count;
	private Thread history_thread;
	private Thread daemon;
	private Thread produce_thread;
	private Thread[] swapall;
	private Thread[] swapdone;

	protected BasicFeeder(String name) {
		super();
		_name = name;
		eventbox = new QueuedEventBox();// default set capacity to
										// Integer.MAX_VALUE
		adapters = new ArrayList<IAdapter>();
		all = new LinkedBlockingQueue<Event>();
		pending = new LinkedBlockingQueue<Event>();
		done = new LinkedBlockingQueue<String>();
		flag = new ConcurrentHashMap<String, String>();
		waitingReply = new ConcurrentHashMap<String, WaitingEvent>();

		dirhome = (String) conf.get(EConfig.EDA_FEEDER_DIR_HOME);
		swapall_count = ((Integer) conf.get(EConfig.EDA_PIVOT_SWAPALL_COUNT))
				.intValue();
		swapdone_count = ((Integer) conf.get(EConfig.EDA_PIVOT_SWAPDONE_COUNT))
				.intValue();
		init();
	}

	private void init() {

		initZMQ();

		if (!FileUtil.isDirEmpty(dirhome + File.separator + DIR_ALL, EXT)) {
			recover();
		}
//		// when invoke by ClientFeeder,the following lines should be deleted.
//		PatAdmitAdapter pat_adapter = new PatAdmitAdapter();
//		setAdapter(pat_adapter);
//		try {
//			startFeeding();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

	}

	private void initZMQ() {

		_dest = "tcp://" + (String) conf.get(EConfig.EDA_PIVOT_INCOMING_HOST)
				+ ":" + (Integer) conf.get(EConfig.EDA_PIVOT_INCOMING_PORT);

		context = ZMQ.context(1);
		socket = context.socket(ZMQ.REQ);
		socket.setIdentity(_name.getBytes());
		poller = context.poller(1);
	}

	private void recover() {
		LOG.info("Feeder now starts recovering...");

		HistoryWork hiswork = new HistoryWork(dirhome, DIR_ALL, DIR_FORK, EXT,
				all, pending);
		history_thread = new Thread(hiswork);
		history_thread.setDaemon(true);
		history_thread.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			LOG.error(e);
		}

	}

	@Override
	public void run() {
		try {

			daemon = new Thread(new FeederDaemon());
			daemon.setDaemon(true);
			daemon.start();
			Thread.sleep(1000);

			feed();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void feed() throws InterruptedException, IOException {

		LOG.debug("starts to send event to pivot... ");
		sendEvent();

		// poller.register(socket,Poller.POLLIN);

		while (!Thread.currentThread().isInterrupted()) {

			poller.register(socket, Poller.POLLIN);
			poller.poll(3000 * 1000);

			// poller.poll();
			if (poller.pollin(0)) {

				String replyid = new String(socket.recv(0));
				LOG.debug("get reply for event with id  [" + replyid + "]");
				getReply(replyid);

				LOG.debug("send event to the pivot...");
				sendEvent();
				Thread.sleep(1000);

				poller.unregister(socket);
			} else {
				LOG.info("no relpy from the Pivot, try to re_connect...");
				socket.setLinger(0);
				socket.close();
				poller.unregister(socket);
				socket = context.socket(ZMQ.REQ);
				socket.setIdentity(_name.getBytes());
				socket.connect(_dest);

				sendEvent();

			}

		}
		stopFeeding();
	}

	private void sendEvent() throws IOException, InterruptedException {
		Event evt = pending.take();
		byte[] request_data = JDKSerializeUtil.getBytes(evt);
		socket.send(final_dest.getBytes(), ZMQ.SNDMORE);
		socket.send("".getBytes(), ZMQ.SNDMORE);
		socket.send(request_data, 0);
		waitReply(evt);
	}

	private void waitReply(Event evt) {
		String evtId = evt.getEventID().toString(); // be careful about String
													// type
		WaitingEvent wevt = new WaitingEvent(evt);
		waitingReply.put(evtId, wevt);
	}

	private void getReply(String reEvtId) throws InterruptedException {
		waitingReply.remove(reEvtId);
		done.put(reEvtId);
	}

	@Override
	public void startFeeding() throws Exception {

		if (this.isAlive()) {
			LOG.warn("This thread is already started!");
		} else {
			if (adapters == null || adapters.size() < 1) {
				throw new Exception("no adapter attached to me!");
			}
			prepareAdapters();
			socket.connect(_dest);
			Thread.sleep(1000);
			super.start();
		}
	}

	// when using this method?
	@Override
	public void stopFeeding() {
		socket.close();
		context.term();
		super.interrupt();
	}

	private void prepareAdapters() {
		for (IAdapter adapter : adapters) {
			adapter.setFeedStream(eventbox);
			adapter.startAdapter(); // active adapter to produce event
		}
	}

	/**
	 * allow duplicate adapters to be added
	 */
	public void setAdapter(IAdapter a) {
		adapters.add(a);
	}

	class FeederDaemon implements Runnable {

		final static int DEFAULT_PULSE = 1000;
		int _pulse;

		FeederDaemon() {
			this(DEFAULT_PULSE);
		}

		FeederDaemon(int pulse) {
			_pulse = pulse;
		}

		@Override
		public void run() {

			startProduceEvent();
			startSwapEvent();
			while (!Thread.currentThread().isInterrupted()) {
				try {
					// buildEvent();
					disposeWaitReplyEvent();
					detectThreads();
					Thread.sleep(_pulse);

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

		private void startProduceEvent() {
			produce_thread = new Thread(new EventProduce());
			produce_thread.start();
		}

		private void startSwapEvent() {
			swapall = new Thread[swapall_count];
			for (int i = 0; i < swapall_count; i++) {
				Swap sall = new Swap(Swap.MODE_ALL, EXT, dirhome, DIR_ALL,
						DIR_DONE, all, flag);
				swapall[i] = new Thread(sall);
				swapall[i].start();
			}
			swapdone = new Thread[swapdone_count];
			for (int i = 0; i < swapdone_count; i++) {
				Swap sdone = new Swap(Swap.MODE_DONE, EXT, dirhome, DIR_ALL,
						DIR_DONE, done, flag);
				swapdone[i] = new Thread(sdone);
				swapdone[i].start();
			}
		}

		private void disposeWaitReplyEvent() throws InterruptedException {
			if (!waitingReply.isEmpty()) {
				Set<Entry<String, WaitingEvent>> waitingEvtSet = waitingReply
						.entrySet();
				for (Entry<String, WaitingEvent> waitingEvtEntry : waitingEvtSet) {
					WaitingEvent wevt = waitingEvtEntry.getValue();
					if (wevt.isExpired()) {
						LOG.debug("event with id [" + waitingEvtEntry.getKey()
								+ "] waiting reply time out, requeue it...");
						pending.put((Event) wevt);
						waitingReply.remove(waitingEvtEntry.getKey());
					}
					// AtomicE ae = (AtomicE) wevt.getEvtData();
					// eventbox.put(ae);
				}

			}
		}

		private void detectThreads() {

			for (Thread t : swapall) {
				if (!t.isAlive()) {
					LOG.info("swapall thread:" + t.getName() + " is dead!!");
				}
			}
			for (Thread t : swapdone) {
				if (!t.isAlive()) {
					LOG.info("swapdone thread:" + t.getName() + " is dead!!");
				}
			}

			if (!produce_thread.isAlive()) {
				LOG.info("produce thread:" + produce_thread.getName()
						+ " is dead!!");
			}
		}

	}

	class EventProduce implements Runnable {

		Event evt;

		@Override
		public void run() {
			LOG.debug("start producing events and put them into the queue...");
			while (!Thread.currentThread().isInterrupted()) {
				try {
					evt = buildEvent();
					all.put(evt);
					pending.put(evt);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}

		private Event buildEvent() throws InterruptedException,
				UnknownHostException {

			Event e;
			AtomicE ae;
			ae = eventbox.take();
			e = EventBuilder.buildEvent(ae);
			// e.setEventID(genEventID());
			return e;

		}
		
	}

//	public static void main(String[] args) {
//		new FeederTest("feeder1");
//
//	}

}
