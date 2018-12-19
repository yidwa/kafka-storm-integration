// TODO Auto-generated constructor stub
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
		 * retrieve the physical distance between two nodes and send out saperately, but only send one path once
		 * @author yidwa
		 *
		 */
		public class PathDistanceBolt extends BaseRichBolt{
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
				String p = (String) input.getValue(0);
				String[] temp = p.split(" ");
				ArrayList<String> paths = new ArrayList<>();
				for(String s: temp){
					paths.add(s);
				}
				ArrayList<String> dd = updateDis(paths);
				String tempoutput = "";
				for(int i = 0; i<paths.size();i++){
					tempoutput+=paths.get(i)+" "+dd.get(i)+",";
				}
				collector.emit(new Values(tempoutput));
				collector.ack(input);
			}
			
			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				// TODO Auto-generated method stub
				declarer.declare(new Fields("pathD"));
			}

			public ArrayList<String> updateDis(ArrayList<String> nodes){
				ArrayList<String> p = new ArrayList<>();
				ArrayList<String> d = new ArrayList<>();
		
				try {
					File f = new File(Variables.path+"distance");
					FileReader fr = new FileReader(f);
					BufferedReader br = new BufferedReader(fr);
				    String line = br.readLine();
				  
				    while(line!=null){
				    	String[] t = line.split(" ");
				    	p.add(t[0]);
				    	d.add(t[1]);
				    }
					} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					System.out.println("file does not exist");
				} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			
				ArrayList<String> result = new ArrayList<>();
				for(String s:nodes){
					int i = p.indexOf(s);
					result.add(d.get(i));
				}
				return d;
			}


		    public void updateInfo(String info){
		    	
		    		FileWriter fw;
					try {
						File f = new File(Variables.path+"path");
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

