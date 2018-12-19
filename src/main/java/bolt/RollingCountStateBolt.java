package bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import com.esotericsoftware.minlog.Log;

import general.Method;

/**
 * counting the no.of each item or node by reading all data collected from past certain time interval
 * @author yidwa
 *
 */
public class RollingCountStateBolt extends BaseStatefulBolt<KeyValueState<String, Long>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
	private static final int NUM_WINDOW_CHUNKS = 5;
	private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 12;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 60;
	private final SlidingWindowCounter<Object> counter;
	private final int windowLengthInSeconds;
	private final int emitFrequencyInSeconds;
	private OutputCollector collector;
	private NthLastModifiedTimeTracker lastModifiedTracker;
	private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
		      "Actual window length is %d seconds when it should be %d seconds"
		          + " (you can safely ignore this warning during the startup phase)";
	
	private KeyValueState<String, Long> wordCounts;
	
	
	public RollingCountStateBolt() {
		// TODO Auto-generated constructor stub
		this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}
	
	
	public RollingCountStateBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
		// TODO Auto-generated constructor stub
		this.windowLengthInSeconds = windowLengthInSeconds;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
	}
  
	 private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
		    return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
		  }




	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
	}
	

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(TupleUtils.isTick(input)){
//			System.out.println("received tick tuple, triggering emit of current window counts");
//			Log.debug("received tick tuple, triggering emit of current window counts");
			emitCurrentWindowCounts(input);
		}
		else{
			countObjAndAck(input);
		}
		collector.ack(input);	
	}
	
	private void emitCurrentWindowCounts(Tuple input){
		Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
//		for(Object obj: counts.keySet()){
//			wordCounts.put((String)obj, counts.get(obj));
//		};
		int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
		lastModifiedTracker.markAsModified();
		if (actualWindowLengthInSeconds != windowLengthInSeconds){
			LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
		}
		emit(input, counts, actualWindowLengthInSeconds);
	}
	
	  private void emit(Tuple input, Map<Object, Long> counts, int actualWindowLengthInSeconds) {
//		  String s = "";
		  
		    for (Entry<Object, Long> entry : counts.entrySet()) {
		      Object obj = entry.getKey();
		      String send = obj.toString();
		      Long count = entry.getValue();
		      wordCounts.put(send, count);
		      collector.emit(input, new Values(send, count, actualWindowLengthInSeconds));
//		      System.out.println("emit from couting "+send+" , "+count);
//		      s+=(String)obj+" , "+count;
//		      s+="\n";
		    }
//		    collector.ack(input);
//		   Method.writeFile(s, counting", false);
		  }

	  
	  private void countObjAndAck(Tuple tuple) {
//		    Object obj = tuple.getValue(0);
		    String k = (String) tuple.getValue(0);
		  	counter.incrementCount(k);
//		    collector.ack(tuple);
		  }

	  
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		  declarer.declare(new Fields("word", "count", "actualWindowLengthInSeconds"));
	  }

	  @Override
	  public Map<String, Object> getComponentConfiguration() {
	    Map<String, Object> conf = new HashMap<String, Object>();
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
	    return conf;
	  }


	@Override
	public void initState(KeyValueState<String, Long> state) {
		// TODO Auto-generated method stub
		wordCounts = state;
	}

}
