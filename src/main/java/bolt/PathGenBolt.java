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
import java.util.TreeSet;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import general.Variables;


/**
 * send out all possible routes for the existing observed node or location
 * @author yidwa
 *
 */
public class PathGenBolt extends BaseRichBolt{
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
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String node = (String) input.getValue(0);
		ArrayList<String> path = pathGen(node);
//		updateInfo(path, "G_for_checking");
//		System.out.println("path gen receiving "+node);
		
		for(String p : path){
			collector.emit(input, new Values(p));
		}
	
//		updateInfo(tempoutput);
		
		collector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("path"));
	}

	/**
	 * finding possible route for the given node
	 * @param nodes
	 * @return
	 */
	public ArrayList<String> pathGen(String node){
		ArrayList<String> result = new ArrayList<>();
		try {
			File f = new File(Variables.path+"PATH");
//			File f = new File("/Users/yidwa/Desktop/path");
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
		    String line = br.readLine();
		    String[] templist;
	
		    
		    if(line!=null){
		        templist = line.split(" ");
		        for(String s:templist){
		        	if(s.split("-")[0].equals(node))
		        		result.add(s);
		        }
		    }
		    br.close();
			} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("path file does not exist");
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return result;
	}


//    public void updateInfo(ArrayList<String> arr, String path){
//    	
//    		FileWriter fw;
//			try {
//				File f = new File(Variables.path+path);
//				fw = new FileWriter(f,true);
//				BufferedWriter bw = new BufferedWriter(fw);
//				for(String s: arr){
//					bw.write(s+" ");
//					bw.write("\n");
//				}
//	    		bw.flush();
//	    		bw.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    	
//    	
//    }
}
