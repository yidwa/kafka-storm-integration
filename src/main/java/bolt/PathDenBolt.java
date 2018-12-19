package bolt;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

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
 * send out all possible routes for the existing observed node or location
 * @author yidwa
 *
 */
public class PathDenBolt  extends BaseRichBolt{
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
		if(TupleUtils.isTick(input)){
			ArrayList<String> pathlist = pathGget();
			HashMap<String, Long> records = logUpdate();
			if(!records.isEmpty()&& pathlist.size()>0){
				for(String s : pathlist){
					if(s.contains("-")){
						String[] ab = s.split("-");
						String start = ab[0];
						String end = ab[1];
						if(records.containsKey(start)&& records.containsKey(end)){
							//need to check if the count is for 120s
							double den = 1.0*(records.get(start)+records.get(end))/(2*120);
//	    			
							DecimalFormat formatter  = new DecimalFormat("#0.00");
//							den = Double.valueOf(formatter.format(den));
							String sd = formatter.format(den);
							String emits = s+" "+sd;
							collector.emit(input, new Values(emits));
//							System.out.println("emitting from Path d for path "+s+" " + emits);
						}
					}
				}
			}
		}
		else{
    		if(input.getSourceComponent().equals("counting")){
//    			System.out.println("receiving in path vel from path D");
    	
    			HashMap<String, Long> records;
				
				records = logUpdate();
				
    			long count = input.getLongByField("count");
    			String word = input.getStringByField("word");
    			records.put(word,count);
//    			System.out.println("write "+p+" , "+d+" , into records");
    			updateInfo(records,"count_log");
//    			System.out.println("update done for vel");
    		}
    		else if(input.getSourceComponent().equals("pathG")){
    			ArrayList<String> pathlist = pathGget();
    			String s = (String) input.getStringByField("path");
    			if(!pathlist.contains(s)){
    				pathlist.add(s);
    				updatePathG(pathlist);
    			}
//    			String pathname = s;
//    			String[] temp = s.split("-");
//    			String start = "";
//    			String end = "";
//    			if(temp.length>1){
//    				start=temp[0];
//    				end = temp[1];
//    			}
//    			double den = 0;
//    			HashMap<String, Long> records = logUpdate();
//    			if(records.containsKey(start) && records.containsKey(end))
//    				den += records.get(start)+records.get(end);
////    			if(records.containsKey(end))
////    				den += records.get(end);
//    			den = den*1.0/2;
//    			
//    			DecimalFormat formatter  = new DecimalFormat("#0.00");
//    			den = Double.valueOf(formatter.format(den));
//    			
////    			HashMap<String, Double> dens = denUpdate(); 
//    			if(den>0 && pathname.contains("-")){
////    				dens.put(pathname, den);
////    				updateInfoS(dens, "den_for_checking");
//    				collector.emit(new Values(pathname,den));
//    				System.out.println("emitting from Path d for path "+pathname+" , "+den);
//    			}
////    			
    		} 
		}

    		collector.ack(input);
    
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("avgdensity"));
	}

	/**
	 * finding possible route for the given node
	 * @param nodes
	 * @return
	 * @throws IOException 
	 */
	public HashMap<String, Long> logUpdate() {
		HashMap<String, Long> result= new HashMap<>();
		try {
			File f = new File(Variables.path+"count_log");
			f.createNewFile();
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();

			while(line!=null){
				String[] temp = line.split(" ");
				if(temp.length>1)
					result.put(temp[0], Long.valueOf(temp[1]));
				line = br.readLine();
			}
			br.close();
			fr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("count_log file does not exist");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	
	public void updatePathG(ArrayList<String> path){
		try {
			File f = new File(Variables.path+"pathG");
			f.createNewFile();
			FileWriter fw = new FileWriter(f);
			BufferedWriter bw = new BufferedWriter(fw);

			for(String s: path){
				bw.write(s);
				bw.write("\n");
			}
			bw.flush();
			bw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	
	public ArrayList<String> pathGget(){
		
		ArrayList<String> result = new ArrayList<>();
		try {
			File f = new File(Variables.path+"pathG");
			f.createNewFile();
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
		    String line = br.readLine();
		 
		    while(line!=null){
		        result.add(line);
		        line = br.readLine();
		    }
		    br.close();
		    fr.close();
			} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("path file does not exist");
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return result;
	}
//	public void updateInfoS(HashMap<String, Double> log, String filename){
//
//		FileWriter fw;
//		try {
//			File f = new File(Variables.path+filename);
//			fw = new FileWriter(f);
//			BufferedWriter bw = new BufferedWriter(fw);
//			for(String s: log.keySet()){
//				bw.write(s+" "+log.get(s)+"\n");
//			}
//			bw.write("\n");
//			bw.flush();
//			bw.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//
//	}
	

	public void updateInfo(HashMap<String, Long> log, String filename){

		FileWriter fw;
		try {
			File f = new File(Variables.path+filename);
//			f.createNewFile();xw
			fw = new FileWriter(f);
			BufferedWriter bw = new BufferedWriter(fw);
			for(String s: log.keySet()){
				bw.write(s+" "+log.get(s)+"\n");
			}
			bw.write("\n");
			bw.flush();
			bw.close();
		} catch (IOException e) {
			System.out.println("no file found for "+filename);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
}
