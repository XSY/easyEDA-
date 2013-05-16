package easyeda.app;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


@SuppressWarnings("rawtypes")
public class BaseDataBolt extends BaseRichBolt{

	
	private static final long serialVersionUID = -9101655433930037756L;
	
	public static Logger LOG = Logger.getLogger(BaseDataBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		
	}

	@Override
	public void execute(Tuple input) {
				
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
				
	}

}
