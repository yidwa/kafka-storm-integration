package bolt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
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

public class LogTimeBolt extends BaseRichBolt{
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
			conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
			return conf;
		}
		
		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub	
			if(TupleUtils.isTick(input)){
				backupInfo(getRecords());
			}
			else{
			String p = input.getStringByField("path");
			Double d = input.getDoubleByField("time");

//			System.out.println("recevingi log "+p+" , "+d);
			HashMap<String, Double> records = getRecords();
			records.put(p, d);
			
		
			updateInfo(records,"final_timing_log");
//			System.out.println("emitting from timing with path "+p+" , "+time);
		
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("time"));
		}



		public void backupInfo(HashMap<String, Double> records){
			Socket socket;
		    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
		    Timestamp ts = new Timestamp(System.currentTimeMillis());
			String info = sdf.format(ts)+"\n";
			
			try{
				socket = new Socket("115.146.86.60", 9999);
				info+= socket.getLocalAddress()+"\n";
				DataOutputStream os = new DataOutputStream(socket.getOutputStream());
				for(String s: records.keySet()){
					info+= s+" "+records.get(s);
					info+="\n";
				}
				OutputStreamWriter osw = new OutputStreamWriter(os);
				osw.write(info);
				osw.flush();
				os.flush();
				os.close();
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
		
		
		
		public HashMap<String, Double> getRecords(){
			HashMap<String, Double> result = new HashMap<>();
			
			try {
				File f = new File(Variables.path+"final_timing_log");
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
				System.out.println("timing file does not exist");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return result;
		}

		public void updateInfo(HashMap<String, Double> log, String filename){

			FileWriter fw;
			try {
				File f = new File(Variables.path+filename);
				
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
				e.printStackTrace();
			}


		}
	}
