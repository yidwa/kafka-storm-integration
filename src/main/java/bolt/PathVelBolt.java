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

public class PathVelBolt extends BaseRichBolt{
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
//		System.out.println("receviing input in Path velocity bolt from "+input.getSourceComponent());
//		if(input.getSourceComponent().equals("pathD")){
		
			
//			System.out.println("receive tuples not ticked");
			HashMap<String, Double> records = logUpdate();
			String[] read = input.getStringByField("avgdensity").split(" ");
			if(read!=null){
				String p = read[0];
				Double ad = Double.valueOf(read[1]);
//			System.out.println("receivie in PathV"+p+ " , "+d);
				records.put(p,ad);
//			System.out.println("update density for "+p+" , "+ad+" , into records");
				updateInfo(records,"den_log");
			
				double v = 0.1;
				if(ad!=0) {
					double t = ad/0.6;
					v = ad * (1 - Math.exp(-1.913 * (1/t - 1/5.4)));
				}
				DecimalFormat formatter  = new DecimalFormat("#0.00");
				v = Double.valueOf(formatter.format(v));
				collector.emit(input, new Values(p,v));
				System.out.println("emit from PathV "+p+" , "+v);
		

				collector.ack(input);
			}
			
//			System.out.println("update done for vel");
//		}
//		else if(input.getSourceComponent().equals("pathG")){
//			String s = (String) input.getValue(0);
//			double den = getAvgDensity(s);
//			//need to change to the referred formular
//			double v = 1.33 * den;
//			DecimalFormat formatter  = new DecimalFormat("#0.00");
//			v = Double.valueOf(formatter.format(v));
//			collector.emit(new Values(s,v));
////			System.out.println("emitting from Path velocity for path "+s+" , "+v);
//		} 
	

		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("path","velocity"));
	}

	public HashMap<String, Double> logUpdate(){
		HashMap<String, Double> result= new HashMap<>();
		try {
			File f = new File(Variables.path+"den_log");
			f.createNewFile();
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();

			while(line!=null){
				String[] temp = line.split(" ");
				if(temp.length>1)
					result.put(temp[0], Double.valueOf(temp[1]));
				line = br.readLine();
			}
			br.close();
			fr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	
	public  HashMap<String, Double> getDis(){
		HashMap<String, Double> dis = new HashMap<>();
		try {
			File f = new File(Variables.path+"DISTANCE");
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
		    String line = br.readLine();
		  
		    while(line!=null){
		    	String[] t = line.split(" ");
		    	dis.put(t[0], Double.valueOf(t[1]));
		    }
			} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("file does not exist");
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		return dis;
	}
	

//	public Double getAvgDensity(String path){
//		double avgden = 0;
//		try {
//			File f = new File(Variables.path+"density_log");
//			FileReader fr = new FileReader(f);
//			BufferedReader br = new BufferedReader(fr);
//			String line = br.readLine();
//
//			while(line!=null){
//				String[] temp = line.split(" ");
//				if(temp.length>1 && temp[0].equals(path)){
//					avgden = Double.valueOf(temp[1]);
//					break;
//				}
//				line = br.readLine();
//			}
//			br.close();
//			fr.close();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			System.out.println("density file does not exist");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return avgden;
//	}

	public void updateInfo(HashMap<String, Double> log, String filename){

		FileWriter fw;
		try {
			File f = new File(Variables.path+filename);
			f.createNewFile();
			fw = new FileWriter(f);
			BufferedWriter bw = new BufferedWriter(fw);
			for(String s: log.keySet()){
				bw.write(s+" "+log.get(s)+"\n");
			}
			bw.write("\n");
			bw.flush();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
//			System.out.println("just create file "+filename);
			e.printStackTrace();
		}


	}
}
