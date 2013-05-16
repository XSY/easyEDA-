package ken.event.client.follower;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import ken.event.EConfig;
import ken.event.Event;
import ken.event.bus.SocketID;
import ken.event.client.adapter.IAdapter;
import ken.event.util.JDKSerializeUtil;
import ken.event.util.ZMQUtil;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

/**
 * AdvancedFollower usually works together with customer [thing]s like storm-way
 * 
 * @author KennyZJ
 * 
 */

@SuppressWarnings("rawtypes")
public class AdvancedFollower extends Thread implements IFollower {

	public static Logger LOG = Logger.getLogger(AdvancedFollower.class);

	private String _fo_id;
	private String _key;
	private LinkedBlockingQueue<Event> _evt_queue;
	
	private String _dest;

	ZMQ.Context _context;
	ZMQ.Socket _socket;
	ZMQ.Poller _poller;

	public AdvancedFollower(String fid, String key,
			LinkedBlockingQueue<Event> queue) throws IOException {
		super();
		_fo_id = fid;
		_key = key;// follower name(key)
		_evt_queue = queue;
		init();
	}
	


	private void init() throws IOException {

		Map<String, Object> conf = EConfig.loadAll("easyEDA.yaml");
		
		_dest = "tcp://" + conf.get(EConfig.EDA_ROUTER_OUTGOING_HOST) + ":"
				+ conf.get(EConfig.EDA_ROUTER_OUTGOING_PORT);

		_context = ZMQ.context(1);
		_socket = _context.socket(ZMQ.REQ);
		SocketID sock_id = new SocketID(_key, _fo_id);
		_socket.setIdentity(JDKSerializeUtil.getBytes(sock_id));
		
		_poller = _context.poller(1);
	}

	public void startFollowing() throws Exception {
		if (this.isAlive()) {
			LOG.warn("This thread is already working!");
		} else {
			_socket.connect(_dest);
//			_socket.send("READY".getBytes(), 0);
			Thread.sleep(100);
			super.start();
		}
	}

	@Override
	public void stopFollowing() {
		_socket.close();
		_context.term();
		super.interrupt();
	}

	@Override
	public void setAdapter(IAdapter a) {
		// do nothing
	}

	@Override
	public void run() {
		try {
			follow();
		} catch (IOException ioe) {
			LOG.error("ioexception: " + ioe.getMessage());
		} catch (ClassNotFoundException cnfe) {
			LOG.error("classnotfoundexception:" + cnfe.getMessage());
		} catch (InterruptedException ire) {
			LOG.error("interruptedexception:" + ire.getMessage());
			Thread.currentThread().interrupt();
		}
	}

	private void follow() throws IOException, ClassNotFoundException,
			InterruptedException {
		int count = 0;
		byte[] payload;
		
		ZMQUtil.report(_socket, "ready");
		while (!Thread.currentThread().isInterrupted()) {
			
			_poller.register(_socket,Poller.POLLIN);
//			_poller.poll(8000 * 1000);
			if(_poller.pollin(0)){
				
				try{
					String client_addr = new String(_socket.recv(0));
					LOG.debug("client_addr:" + client_addr);
					String empty = new String(_socket.recv(0));
					assert empty.length() == 0 | true;

					payload = _socket.recv(0);
					LOG.debug("payload.length = " + payload.length);
					Event e = (Event) JDKSerializeUtil.getObject(payload);
					_evt_queue.put(e);
											
					String msgId = client_addr;
					_socket.send(msgId.getBytes(), 0);
//					_socket.send(msgId.getBytes(), ZMQ.SNDMORE);
//					_socket.send(empty.getBytes(), ZMQ.SNDMORE);
//					_socket.send((_key).getBytes(), 0);// _key as reply
					
					count++;
					LOG.debug(Thread.currentThread().getName()
							+ "[Worker]-[fkey: " + _key + "|tid= " + _fo_id +" received ["
							+ count + "] events!");
				}catch(IOException e){
					LOG.error(e);
				}catch (ClassNotFoundException e){
					LOG.error(e);
				}
				
				
			}
//			else{
//				
//			}
			
			
		}
	}

	@Deprecated
	@Override
	/**
	 * not suggested to use this method, use startDining() instead
	 */
	public synchronized void start() {
		LOG.warn("Not supported any more, this method takes no job to do. Please use startFollowing() instead!");
	}

	@Deprecated
	@Override
	/**
	 * not suggested to use this method, use stopDining() instead
	 */
	public void interrupt() {
		LOG.warn("Not supported any more, this method takes no job to do. Please use stopFollowing() instead!");
	}

}
