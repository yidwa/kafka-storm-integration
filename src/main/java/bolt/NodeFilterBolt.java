package bolt;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import general.Variables;

/**
 * send out the latest whole list of individual node, or location for path construction
 * @author yidwa
 *
 */
public class NodeFilterBolt extends BaseRichBolt{
	  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(PrinterBolt.class);
	  private OutputCollector collector;
	  
	  
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration(){
		Config conf = new Config();
		conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
		return conf;
	}
	
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		ArrayList<String> list = readInfo();
		if(TupleUtils.isTick(input)){
			for(String n: list){
				collector.emit(input,new Values(n));
//				System.out.println("emit from Nodefilter "+ n);
			}
		}
		else{
			String node = (String) input.getValue(0);
		
			boolean needupdate =false;
			if(list.size()==0 || !list.contains(node)){
				list.add(node);
				needupdate = true;
			}
			String update = "";
			for(String n : list){
				update+=n+" ";
//			collector.emit(new Values(n));
			}
			if(needupdate){
				updateInfo(update);
			
			}
			collector.ack(input);
		}
//		collector.emit(new Values(update));
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("node"));
	}

	public ArrayList<String> readInfo(){
		ArrayList<String> result = new ArrayList<String>();
		
		try {
			File f = new File(Variables.path+"node");
			f.createNewFile();
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
		    String line = br.readLine();
		    String[] nodes;
		    
		    if(line!=null){
		        nodes = line.split(" ");
		        for(String n : nodes){
			    	result.add(n);
		        }
		    }
			} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("file does not exist");
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return result;
	}


    public void updateInfo(String info){
    	
    		FileWriter fw;
			try {
				File f = new File(Variables.path+"node");
				f.createNewFile();
				fw = new FileWriter(f);
				BufferedWriter bw = new BufferedWriter(fw);
	    		bw.write(info);
	    		bw.flush();
	    		bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	
    	
    }
}
