package bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBolt extends BaseRichBolt{
	  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(PrinterBolt.class);
	  private OutputCollector collector;
	  
	 
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
//		String coming = (String) input.getValue(0);
		String coming = input.getStringByField("avgdensity");;
		System.out.println("receiving in testing "+coming);
//		collector.emit(new Values(coming));
//		System.out.println("simpleT emit "+coming);
		collector.ack(input);
	}
//	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("location"));
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}





}