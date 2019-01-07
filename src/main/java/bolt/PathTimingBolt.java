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
public class PathTimingBolt extends BaseRichBolt{
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
		String p = input.getStringByField("path");
		Double d = input.getDoubleByField("velocity");
		double distance = disGet(p);
		double time = distance *1.0/d;
		DecimalFormat formatter  = new DecimalFormat("#0.00");
		time = Double.valueOf(formatter.format(time));
		
//		HashMap<String, Double> records = getRecords();
//		records.put(p, time);
		
		collector.emit(input, new Values(p,time));
//		updateInfo(records,"timing_log");
//		System.out.println("emitting from timing with path "+p+" , "+time);
		

		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("path","time"));
	}



	public double disGet(String p){
		double result = 0;
		try {
			File f = new File(Variables.path+"DISTANCE");
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			while(line!=null ){
				String[] temp = line.split(" ");
				if(temp.length>1 && temp[0].equals(p)){
					result = Double.valueOf(temp[1]);
					break;
				}
				line = br.readLine();
			}
			br.close();
			fr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("dis file does not exist");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}


}
