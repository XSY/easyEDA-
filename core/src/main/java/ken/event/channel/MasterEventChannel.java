package ken.event.channel;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import ken.event.EConfig;
import ken.event.Event;
import ken.event.meta.AtomicE;
import ken.event.util.JDKSerializeUtil;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * This is the main channel of event adopting, and it has multiple processing
 * ability. Default use ZMQ REQ-REP type to setup a connection to the client
 * Within the instance of this class, the whole ZMQ environment including
 * context and socket is separated from the one in another event source
 * 
 * @author KennyZJ
 * 
 */
@SuppressWarnings({"serial","rawtypes"})
public class MasterEventChannel extends BaseEventChannel {

	public static Logger LOG = Logger.getLogger(MasterEventChannel.class);

	// message completely processed,reply REPLY_ACK, otherwise REPLY_FAIL for
	// asking pivot to send again
	public final static int REPLY_ACK = 0;
	public final static int REPLY_FAIL = 1;
	
	private LinkedBlockingQueue<Event> evt_queue;
	private ConcurrentHashMap<String,Event> wait_map;
//    private ConcurrentHashMap<String,String> id_map;   //eventid-msgid
//    private LinkedBlockingQueue<String> msg_ids;
    
	private String dest;
    private Thread receEvt;    

	
	public MasterEventChannel() {
		super();
//		evt_queue = new LinkedBlockingQueue<Event>();
//		id_map =  new ConcurrentHashMap<String,String>();
//		msg_ids = new LinkedBlockingQueue<String>();
		
	}

	public MasterEventChannel(boolean isDistributed) {
		super(isDistributed);
		

	}


	@Override
	public void nextTuple() {

		LOG.debug("Spout working...");
		//Event e =  evt_queue.poll();
		try{
			Event evt;
			evt = evt_queue.take();   //blocking
			if(evt != null){
				String id = (String) evt.getEventID();
				LOG.debug("event with id " + id +" is emitted...");
				_collector.emit(new Values(evt), id);	
				wait_map.put(id,evt);
			}
		}catch (InterruptedException e){
			e.printStackTrace();
		}
		
		
				
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("eventdata"));
	}

	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		init(conf);
			
		receEvt = new ReceiveEvents(dest);
		receEvt.start();
		
//		try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//			LOG.error(e.getMessage());
//		}
		
	}

	@Override
	public void ack(Object id) {
				
		LOG.debug("ACK ....");
		wait_map.remove((String)id);
//		String msgId = id_map.remove((String) id);	
//		if(msgId != null){
//			try {				
//				msg_ids.put(msgId);				
//			} catch (InterruptedException e) {				
//				e.printStackTrace();
//			}
//		}
		
		
	}
	
	@Override
	public void fail(Object id) {
		
		LOG.debug("tuple with id "+ id + " failed!");
		Event wait_evt = wait_map.remove((String)id);
		try {
			evt_queue.put(wait_evt);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
//		_socket.send(Integer.toString(REPLY_FAIL).getBytes(), ZMQ.SNDMORE);
//		_socket.send("".getBytes(), ZMQ.SNDMORE);
//		_socket.send(msgId.toString().getBytes(), 0);
	}

	@Override
	public void close() {
		
	}
	
	private void init(Map conf){
		
		evt_queue = new LinkedBlockingQueue<Event>();
		wait_map = new ConcurrentHashMap<String,Event>();
//		id_map =  new ConcurrentHashMap<String,String>();
//		msg_ids = new LinkedBlockingQueue<String>();
		
		dest = "tcp://" + conf.get(EConfig.EDA_PIVOT_OUTGOING_HOST) + ":"
				+ conf.get(EConfig.EDA_PIVOT_OUTGOING_PORT);

	}
	
	class ReceiveEvents extends Thread {
		

		private  String rec_dest;
		private  ZMQ.Context rec_ctx;
		private  ZMQ.Socket rec_socket;		
		private  Poller rec_poller;
		
				
		ReceiveEvents(String dest){
			
			rec_dest = dest;
			zmqInit();
		}
	    
		
		private void zmqInit(){
			
			if (rec_ctx == null) {
				rec_ctx = ZMQ.context(1);
			}
			if (rec_socket == null) {
				rec_socket = rec_ctx.socket(ZMQ.REP);
				
			}
			if (rec_poller == null) {
				rec_poller = rec_ctx.poller(1);
				rec_poller.register(rec_socket, Poller.POLLIN);
			}

			LOG.debug("masterEventChannel connecting to: " + rec_dest + "...");
			rec_socket.connect(rec_dest);
		}
		@SuppressWarnings("unchecked")
		@Override
		public void run(){
			
			LOG.debug("receive msgs from  pivot...");
			Event evt;
			int i = 0; // event receive counter
			while (!Thread.currentThread().isInterrupted()) {
							
				rec_poller.poll(3000*1000);

				if (rec_poller.pollin(0)) {

					try {
						
//						//check if need to send reply first, then ready to receive
//						if(!msg_ids.isEmpty()){
//							
//							sendReply();
//						}
						
						String msgId = new String(rec_socket.recv(0));
						rec_socket.recv(0);
						byte[] request = rec_socket.recv(0);					
						evt = (Event<AtomicE>) JDKSerializeUtil.getObject(request);
//						String evtId = (String) evt.getEventID();
						
						evt_queue.put(evt);
//						id_map.put(evtId, msgId);
						
						i++;
						LOG.debug("No. " + i + " event received with msg id "+ msgId);
						
						//use next reply type temporarily
						LOG.debug("Reply ....");
						rec_socket.send(Integer.toString(REPLY_ACK).getBytes(), ZMQ.SNDMORE);
						rec_socket.send("".getBytes(), ZMQ.SNDMORE);
						rec_socket.send(msgId.toString().getBytes(), 0);
						
//						//send reply
//						if(!msg_ids.isEmpty()){
//							
//							sendReply();
//						}
																		
					} catch (IOException ioe) {
						LOG.error("easyEDA cannot deserialize input event for reason: "
								+ ioe.getCause());
					} catch (ClassNotFoundException cnfe) {
						LOG.error("easyEDA cannot deserialize input event for reason: "
								+ cnfe.getCause());
					} 
					catch (InterruptedException e) {

						e.printStackTrace();
					}

				}
				 else{
				
				 LOG.debug("No message for a while, try to connect again...");
				 rec_poller.unregister(rec_socket);
				 rec_socket.setLinger(0);
				 rec_socket.close();
				 rec_socket = rec_ctx.socket(ZMQ.REP);
				 rec_socket.connect(dest);
				 rec_poller = rec_ctx.poller(1);
				 rec_poller.register(rec_socket, Poller.POLLIN);
				 
				 }
				
			}
			close();
		}
		
//		private void sendReply() {
//			
//			LOG.debug("Reply pivot....");
//			String msgId = msg_ids.poll();
//			rec_socket.send(Integer.toString(REPLY_ACK).getBytes(), ZMQ.SNDMORE);
//			rec_socket.send("".getBytes(), ZMQ.SNDMORE);
//			rec_socket.send(msgId.toString().getBytes(), 0);
//				
//	   }
		
		private void close(){
			
			if (rec_socket != null) {
				rec_socket.close();
				rec_socket = null;
			}
			if (rec_ctx != null) {
				rec_ctx.term();
				rec_ctx = null;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage());
			}
		}
		
		
	}

}
