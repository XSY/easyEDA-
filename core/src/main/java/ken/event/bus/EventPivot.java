package ken.event.bus;

import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import ken.event.EConfig;
import ken.event.Event;
import ken.event.HistoryWork;
import ken.event.Swap;

import ken.event.channel.MasterEventChannel;
import ken.event.meta.PatAdmitEvent;
import ken.event.util.FileUtil;
import ken.event.util.JDKSerializeUtil;
import ken.event.util.ZMQUtil;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

/**
 * EventsPivot switches information between Feeder and Channel.
 * 
 * @author xu shuyan
 * 
 */
public class EventPivot implements Runnable {

	public static Logger LOG = Logger.getLogger(EventPivot.class);
	public final static String EXT = ".msg";
	public final static String DIR_ALL = "waiting";
	public final static String DIR_DONE = "done";
	public final static String DIR_FORK = "fork-history";

	private final static Map<String, Object> conf = EConfig
			.loadAll("easyEDA.yaml");

	private int swapall_count;
	private int swapdone_count;
	private Thread[] swapAll_threads;
	private Thread[] swapDone_threads;

	private String dirhome;
	private int front_port;
	private int back_port;

	private Context front_context;
	private Context back_context;
	private Poller poll_front;
	private Poller poll_back;
	private Socket frontend;
	private Socket backend;

	private Thread rec_thread;
	private Thread daemon;
	private Thread history_thread;
	/**
	 * store all received messages and it will be used for swapping.
	 */
	private LinkedBlockingQueue<Message> all;

	/**
	 * store messages that are well-processed and it will be used for swapping.
	 */
	private LinkedBlockingQueue<String> done;
	/**
	 * mark the message with its Id. It will be used for swapping messages.
	 */
	private ConcurrentHashMap<String, String> flag;
	/**
	 * store messages that are just received but not sent yet.
	 */
	private LinkedBlockingQueue<Message> pending;

	/**
	 * store messages that are already sent, but no reply yet.
	 */
	private ConcurrentHashMap<String, WaitingMessage> waitingReply;

	public EventPivot() {

		dirhome = (String) conf.get(EConfig.EDA_PIVOT_DIR_HOME);
		front_port = ((Integer) conf.get(EConfig.EDA_PIVOT_INCOMING_PORT))
				.intValue();
		back_port = ((Integer) conf.get(EConfig.EDA_PIVOT_OUTGOING_PORT))
				.intValue();
		swapall_count = ((Integer) conf.get(EConfig.EDA_PIVOT_SWAPALL_COUNT))
				.intValue();
		swapdone_count = ((Integer) conf.get(EConfig.EDA_PIVOT_SWAPDONE_COUNT))
				.intValue();

		all = new LinkedBlockingQueue<Message>();
		done = new LinkedBlockingQueue<String>();
		flag = new ConcurrentHashMap<String, String>();
		pending = new LinkedBlockingQueue<Message>();
		waitingReply = new ConcurrentHashMap<String, WaitingMessage>();

		initZMQ();

		if (!FileUtil.isDirEmpty(dirhome + File.separator + DIR_ALL, EXT)) {
			recover();
		} else {
			LOG.info("Every thing is ok! There is no need to recover.");
		}
	}

	private void initZMQ() {

		front_context = ZMQ.context(1);
		back_context = ZMQ.context(1);
		frontend = front_context.socket(ZMQ.ROUTER);
		backend = back_context.socket(ZMQ.DEALER);

		frontend.bind("tcp://*:" + front_port);
		backend.bind("tcp://*:" + back_port);
		// Initialize poll set
		poll_front = front_context.poller(1);
		poll_back = back_context.poller(1);
		poll_front.register(frontend, Poller.POLLIN);
		poll_back.register(backend, Poller.POLLIN);

	}

	/**
	 * recover the pivot to normal state after it crashed
	 */
	private void recover() {
		LOG.info("Pivot now starts recovering...");

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

		LOG.info("Pivot Starts....");

		try {
			daemon = new Thread(new PivotDaemon());
			daemon.setDaemon(true);
			daemon.start();
			Thread.sleep(100);

			rec_thread = new Thread(new MessageReceive());
			rec_thread.start();
			Thread.sleep(100);

			// thread suspends here when no receiver connected.
			sendMsg();
		//	Thread.sleep(500);
		} catch (InterruptedException e) {
			LOG.error("error when start pivot daemon thread" + e);
		}

		while (!Thread.currentThread().isInterrupted()) {

			LOG.debug("all-----------------size() = [" + all.size() + "]");
			LOG.debug("pending-------------size() = [" + pending.size() + "]");
			LOG.debug("waitingReply--------size() = [" + waitingReply.size()
					+ "]");
			LOG.debug("done----------------size() = [" + done.size() + "]");

			poll_back.register(backend, Poller.POLLIN);

			poll_back.poll(4000 * 1000);
			// poll_back.poll();
			if (poll_back.pollin(0)) {
				LOG.debug("backend received reply");

				try {

					getReply();
					sendMsg();
				}
				
				catch (InterruptedException e) {
					LOG.error(e);
				}
				poll_back.unregister(backend);
			} else {
				LOG.info("no relpy from one channel, try to re_bind");
				backend.setLinger(0);
				backend.close();
				poll_back.unregister(backend);
				back_context.term();

				back_context = ZMQ.context(1);
				poll_back = back_context.poller(1);
				backend = back_context.socket(ZMQ.DEALER);

				backend.bind("tcp://*:" + back_port);

				try {
					LOG.debug("send event after re_bind...");
					sendMsg();
				} catch (InterruptedException e) {

					LOG.error(e);
				}

			}
		}

		// We never get here but clean up anyhow, unless this thread is
		// interrupted

		backend.close();
		back_context.term();
	}

	/**
	 * send the message to channel
	 * 
	 * @throws IOException
	 */
	private void sendMsg() throws InterruptedException {
		Message msg = pending.take();

		byte[] part2 = msg.getPart2();
		msg.setPart2(msg.getId()); // channel reply using this part

		// LOG.debug("send.......");
		ZMQUtil.send(msg, backend);
		// revert the message
		// msg.setPart1(part1);
		msg.setPart2(part2);
		waitReply(msg);

	}

	private void waitReply(Message msg) {
		WaitingMessage wmsg = new WaitingMessage(msg);
		String wmsgId = new String(msg.getId());
		waitingReply.put(wmsgId, wmsg);
	}

	/**
	 * Receive reply and according to the type of reply, the corresponding
	 * action would be taken.
	 * 
	 * @param reMsgId
	 * @throws InterruptedException
	 */
	private void getReply() throws InterruptedException {
		String msgSource;
		String reMode;
		String reMsgId;
		Message msg;

		msgSource = new String(backend.recv(0));
		LOG.debug("Get reply for message feeder ----" + msgSource);
		backend.recv(0);
		reMode = new String(backend.recv(0));
		backend.recv(0);
		reMsgId = new String(backend.recv(0));

		int mode = new Integer(reMode);
		
		if (mode == MasterEventChannel.REPLY_ACK) {
			LOG.debug("DONE!");
			waitingReply.remove(reMsgId);
			done.put(reMsgId);
		} else if (mode == MasterEventChannel.REPLY_FAIL) {
			LOG.debug("re-send the message with id- " + reMsgId);
			msg = (Message) waitingReply.remove(reMsgId);
			// if not asked to re-send the message immediately,
			// put it into the queue in normal way
			pending.put(msg);
		}

	}

	class MessageReceive implements Runnable {

		@Override
		public void run() {
			LOG.debug("poll frontend...");

			while (!Thread.currentThread().isInterrupted()) {
				poll_front.poll();
				if (poll_front.pollin(0)) {
					LOG.debug("frontend received messages");
					try {
						receNewMsg();
						// Thread.sleep(500);
					}
					// catch (IOException e){
					// LOG.error(e);
					// }
					catch (InterruptedException e) {
						LOG.error(e);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			}
			frontend.close();
			front_context.term();
		}

		/**
		 * receive new messages from the feeder and reply immediately
		 * 
		 * @throws InterruptedException
		 *             , IOException, ClassNotFoundException
		 * 
		 */
		private void receNewMsg() throws InterruptedException, IOException,
				ClassNotFoundException {
			Message msg = ZMQUtil.receive(frontend);
			all.put(msg);
			pending.put(msg);
			sendReply(msg);

		}

		/**
		 * send reply to feeder
		 * 
		 * @param msg
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		private void sendReply(Message msg) throws IOException,
				ClassNotFoundException {

			frontend.send(msg.getPart1(), ZMQ.SNDMORE);
			frontend.send(Message.EMPTY, ZMQ.SNDMORE);
			Event<PatAdmitEvent> evt;
			evt = (Event) JDKSerializeUtil.getObject(msg.getPart3());
			byte[] evtId = evt.getEventID().toString().getBytes();//
			frontend.send(evtId, 0);

		}

	}

	class PivotDaemon implements Runnable {
		final static int DEFAULT_PULSE = 2000;
		int _pulse;

		PivotDaemon() {
			this(DEFAULT_PULSE);
		}

		PivotDaemon(int pulse) {
			_pulse = pulse;
		}

		@Override
		public void run() {
			LOG.info("Pivot daemon thread starts...");
			startSwap();
			while (!Thread.currentThread().isInterrupted()) {

				try {
					LOG.debug("Pivot daemon is running...");
					disposeWaitReplyMsgs();
					detectSwapThreads();
					Thread.sleep(_pulse);
				} catch (InterruptedException e) {
					LOG.error(e);

				}
			}

		}

		/**
		 * start swap threads for swapping messages into files. The number of
		 * threads that are started is decided in the configuration file.
		 */
		private void startSwap() {

			swapAll_threads = new Thread[swapall_count];
			for (int i = 0; i < swapall_count; i++) {
				Swap sall = new Swap(Swap.MODE_ALL, EXT, dirhome, DIR_ALL,
						DIR_DONE, all, flag);
				swapAll_threads[i] = new Thread(sall);
				swapAll_threads[i].start();
			}
			swapDone_threads = new Thread[swapdone_count];
			for (int i = 0; i < swapdone_count; i++) {
				Swap sdone = new Swap(Swap.MODE_DONE, EXT, dirhome, DIR_ALL,
						DIR_DONE, done, flag);
				swapDone_threads[i] = new Thread(sdone);
				swapDone_threads[i].start();
			}
		}

		/**
		 * if waiting reply time out, put the message into the queue to send it
		 * again
		 * 
		 * @throws InterruptedException
		 */
		private void disposeWaitReplyMsgs() throws InterruptedException {
			if (!waitingReply.isEmpty()) {
				Set<Entry<String, WaitingMessage>> waitingMsgSet = waitingReply
						.entrySet();
				for (Entry<String, WaitingMessage> waitingMsgEntry : waitingMsgSet) {
					WaitingMessage wmsg = waitingMsgEntry.getValue();
					if (wmsg.isExpired()) {
						LOG.debug("waiting reply time out, read the message with id "
								+ waitingMsgEntry.getKey());
						pending.put((Message) wmsg);
						waitingReply.remove(waitingMsgEntry.getKey());
					}

				}

			}
		}

		private void detectSwapThreads() {

			for (Thread t : swapAll_threads) {
				if (!t.isAlive()) {
					LOG.info("swapAll thread:" + t.getName() + " is dead!!");
				}
			}
			for (Thread t : swapDone_threads) {
				if (!t.isAlive()) {
					LOG.info("swapDone thread:" + t.getName() + " is dead!!");
				}
			}
		}
	}

	public static void main(String[] args) {

		Thread pivotThread = new Thread(new EventPivot());
		pivotThread.start();

	}
}
