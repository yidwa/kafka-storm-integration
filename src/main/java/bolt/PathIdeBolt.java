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
 * calculate the densitfy per square
 * @author yidwa
 *
 */
public class PathIdeBolt  extends BaseRichBolt{
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
//		System.out.println("received input in PahtavgDensity from "+input.getSourceComponent());
		String p = input.getStringByField("avgdensity");
//		Double d = input.getDoubleByField("density");
	
		System.out.println("receivie in PathIde"+p);

		String[] ptemp = p.split(" ");
		String path="";
		String den="0";
		if(ptemp.length>1){
			path = ptemp[0];
			den = ptemp[1];
		}
//		System.out.println("receiving in PathIde "+path + " and " +den);
		HashMap<String, Double> dis = getDis();
//
		double distance;
		if(!dis.isEmpty() && dis.containsKey(path))
			distance = dis.get(path);
		else 
			distance = 0;
//		System.out.println("distance is "+distance);
		double tempden = 0;
		if(distance!=0)
		   tempden = Double.valueOf(den)/ (distance * 4.0) ;
		
		DecimalFormat formatter  = new DecimalFormat("#0.00");
		tempden = Double.valueOf(formatter.format(tempden));
		collector.emit(new Values(path,tempden));
	    System.out.println("emitting density per square for  path "+ path+ " " +tempden);

	
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("route", "avgden"));
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
		    	if(t.length>1)
		    		dis.put(t[0], Double.valueOf(t[1]));
		    	line = br.readLine();
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
	
}
