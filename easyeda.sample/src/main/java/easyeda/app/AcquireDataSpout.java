package easyeda.app;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;


public class AcquireDataSpout extends BaseRichSpout {

	private static final long serialVersionUID = 4478381780782122189L;
	private SpoutOutputCollector _collector;
	public static Logger LOG = Logger.getLogger(AcquireDataSpout.class);
	

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		
	}

	@Override
	public void nextTuple() {
	
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields(""));
		
	}
	
	@Override
	public void ack(Object id){
		
	}
	
	@Override
	public void fail(Object id){
		
	}

}
