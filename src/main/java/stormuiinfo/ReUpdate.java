package stormuiinfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import general.Method;

public class ReUpdate implements Runnable{

	URL url;
	String hostport;
	HttpURLConnection conn;
	Object obj;
	JSONObject jobj;
	String output;
	BufferedReader br;
	//the historical records of flow from previous round
	HashMap<String,HashMap<String, Long>> flowrecords;
	// keeps the latency(maximum of process or execute) for each operator, this will be used to write as operator demand
	HashMap<String, String> maxmLa;;
	ArrayList<String> topologyid;
	public ReUpdate(String hostport, HashMap<String,HashMap<String, Long>> flowrecords) {
		// TODO Auto-generated constructor stub
		this.hostport = hostport;
		this.obj = null;
		this.jobj = null;
		this.output = "";
		this.flowrecords = flowrecords;
		topologyid = new ArrayList<>();
		maxmLa = new HashMap<String, String>();
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		//		ReUpdate ru = new ReUpdate("http://115.146.86.60:8080");
		//update the topology
		Topologyget();
		Topologyinfo(topologyid);
		Testing(topologyid);
	}


	public void Testing(ArrayList<String> topologyid) {
		HashMap<String, String[]> result = Partition.getOpt(topologyid);
		Partition.writeOpdemand(maxmLa, result);
	}
	public void Connect(String q){
		try {

			String temp = hostport + q;
			url = new URL(temp);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			if (conn.getResponseCode() != 200){
				throw new RuntimeException("Failed : http error code"+ conn.getResponseCode());
			}

			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		}
		catch (MalformedURLException e){
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void Topologyget(){
		Connect("/api/v1/topology/summary");
		try {
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();

				obj = parser.parse(output);
				jobj = (JSONObject)obj;
				JSONArray topo = (JSONArray) jobj.get("topologies");
				for (int i = 0 ; i< topo.size(); i++){
					obj = topo.get(i);
					jobj = (JSONObject) obj;
					String id = (String)jobj.get("id");
					topologyid.add(id);
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//		System.out.println("values " +topologies.toString());
		conn.disconnect();
	}

	// get the active topology info

	public void Topologyinfo(ArrayList<String> topo){
		if (topo.size() == 0)
			System.out.println("no topology is working at the moment");
		else{

			String delaystring = "";
			String flowstring = "";
			String exestring = "";
			String optString = "";
			for(String s : topo){
//				System.out.println("start collect infor for "+s);
				//if this is the first time to see the topology s, initialize the flowrecords
				if(!flowrecords.containsKey(s)){
					HashMap<String, Long> r = new HashMap<>();
					flowrecords.put(s, r);
				}
				String spoutId = "integer";
				//get the operator list
				ArrayList<String> temp = Operatorinfo(s);
				//delay of each operator
				HashMap<String, Double> exedelay = new HashMap<>();
				HashMap<String, Double> prodelay = new HashMap<>();
				//the data communication between two operators
				HashMap<String, Long> flow = new HashMap<>();
				//executor states of the given operator
				HashMap<String, ArrayList<String>> exe = new HashMap<>();
//				System.out.println("check topologyinfo, operator size "+temp.size());
				for(String oid: temp){
					if(!oid.contains(spoutId)){
//						System.out.println("update info for "+oid);
						operatorDetail(s, oid, temp, exedelay, prodelay, flow,exe);
					}
					else{
//						System.out.println("update infor for "+oid);
						spoutDetail(s, oid, temp, exedelay, flow, exe);
						
					}
				}
				System.out.println("finish udpate");
				//				System.out.println("print delay info");

				delaystring += s+"\n";

				for(String node: exedelay.keySet()){
					delaystring += node+ ","+exedelay.get(node)+","+prodelay.get(node);
					delaystring += "\n";
					//					System.out.println(node+" , "+ delay.get(node));
					//					System.out.println();
				}

				
//				System.out.println("inside topologyinfo check the delay string "+delaystring);
				
				flowstring += s+"\n";


				HashMap<String, Long> records = flowrecords.get(s);
				//				System.out.println("print flow info");
				// initiallize it at the beginning
				if(records.size()==0){
					
					for(String node : flow.keySet()){
//						System.out.println("first time updatet for "+node+ " with "+flow.get(node));
						flowstring += node+" "+flow.get(node);
						flowstring += "\n";
					
						records.put(node, flow.get(node));
						
					}
				}
				//the flowrecords is not empty
				else{
					for(String node : flow.keySet()){
						flowstring += node+" "+(flow.get(node)-records.get(node));
						flowstring += "\n";
						
						records.put(node, flow.get(node));
						
					}
				}

				//				System.out.println("print exe info");
				exestring += s+"\n";

				for(String node : exe.keySet()){
					exestring += node +","
							+ "";
					//					System.out.println(node+" , ");
					for(String e : exe.get(node))
						//						System.out.println(e+" , ");
						exestring += e + ",";
					exestring += "\n";
					//				System.out.println();
				}
				optString += s+"\n";
				ArrayList<String> r = optReorder(spoutId, records, exedelay, prodelay);
				for(String opt: r)
					optString += opt+" ";
			}
			// write the records to the file
			Method.writeFile(delaystring, "delay", true);
			Method.writeFile(flowstring, "flow", true);
			Method.writeFile(exestring, "execution", true);
			Method.writeFile(optString, "optReorderHistory", true);
			Method.writeFile(optString, "optReorder", false);
		}
			
	}
	/**
	 * Reorder the operators by WSPT
	 * @return
	 */
	public ArrayList<String> optReorder(String spoutId, HashMap<String, Long> records, HashMap<String, Double> exedelay, HashMap<String, Double> prodelay) {
		HashMap<String, Integer> weights = new HashMap<String, Integer>();
		
		HashMap<String, Integer> m = new HashMap<String, Integer>();
		int p = 1;
		weights.put(spoutId, p);
		ArrayList<String> links = new ArrayList<String>();
		for(String s: records.keySet()) 
			links.add(s);
		while (links.size()>0){
			// find the operators by iterating the flow link
			for(int i = 0; i<links.size(); i++) {
				String l = links.get(i);
				if(l.contains("-") && weights.containsKey(l.split("-")[0])){
					String al = l.split("-")[1];
					p = p+ 1;
					weights.put(al, p);
					links.remove(l);
					break;
				}		
			}
		}
		for(String s: weights.keySet()) {
			double a = exedelay.get(s);
			double r = 0;
			if(s.equals(spoutId))
				r = a;
			else {
				double b = prodelay.get(s);
				r = (a>b)? a:b;
			}
		
			m.put(s, (int) Math.round(r/weights.get(s)));
			maxmLa.put(s, String.format("%.2f", r));
			System.out.println("add a new record to the metrics as "+s+" , "+ weights.get(s)+" , "+  r + " , " + (int) Math.round(r/weights.get(s)));
		}
		ArrayList<String> result = new ArrayList<String>();
		for(String s: sortByComparator(m).keySet())
			result.add(s);
		
		return result;
	}

	 @SuppressWarnings("unused")
	private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap)
	    {

	        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

	        // Sorting the list based on values
	        Collections.sort(list, new Comparator<Entry<String, Integer>>()
	        {
	            public int compare(Entry<String, Integer> o1,
	                    Entry<String, Integer> o2)
	            {
	               
	                    return o1.getValue().compareTo(o2.getValue());
	              
	            }
	        });

	        // Maintaining insertion order with the help of LinkedList
	        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
	        for (Entry<String, Integer> entry : list)
	        {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }
	 
	//	 get the topology operator 
	public ArrayList<String> Operatorinfo(String tid){
		ArrayList<String> ops = new ArrayList<>();
		Connect("/api/v1/topology/" +tid+ "/metrics");
		try {
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				obj = parser.parse(output);
				jobj = (JSONObject)obj;
				JSONArray topo = (JSONArray) jobj.get("spouts");
				Object tobj = topo.get(0);
				JSONObject tjobj = (JSONObject) tobj;
				ops.add((String)tjobj.get("id"));
				topo = (JSONArray) jobj.get("bolts");
				for (int i = 0 ; i< topo.size(); i++){
					tobj = topo.get(i);
					tjobj = (JSONObject) tobj;
					ops.add((String)tjobj.get("id"));
//					System.out.println("inside operatorinfo, collect info for the operator "+tjobj.get("id"));
					//							String host = (String)tjobj.get("host");
					//							Supervisor s = new Supervisor((String)tjobj.get("id"),(Long)tjobj.get("slotsTotal"),
					//							(Long)tjobj.get("slotsUsed"),(Double)tjobj.get("totalMem"),(Double)tjobj.get("totalCpu"),
					//							(Double)tjobj.get("usedMem"),(Double)tjobj.get("usedCpu"));
					////						workers.add(new Supervisor((String)tjobj.get("id"),(String)tjobj.get("host"),(Long)tjobj.get("slotsTotal"),
					////								(Long)tjobj.get("slotsUsed"),(Double)tjobj.get("totalMem"),(Double)tjobj.get("totalCpu"),
					////								(Double)tjobj.get("usedMem"),(Double)tjobj.get("usedCpu")));
					//							workers.put(host, s);
				}
				//						
				//						//System.out.println(output+ "\n");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//
		//			
		//////			System.out.println(System.nanoTime());
		////			System.out.println("workers status now");
		////			for(Entry<String, Supervisor> s : workers.entrySet())
		////				System.out.println(s.getValue().toString());
		//			
		conn.disconnect();
		return ops;
		//		
	}


	public void spoutDetail(String tid, String oid, ArrayList<String> olist, HashMap<String, Double> delay, 
			HashMap<String, Long> flow, HashMap<String, ArrayList<String>> exe){
		Connect("/api/v1/topology/"+ tid+ "/component/"+ oid);
		try{
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				obj = parser.parse(output);
				jobj = (JSONObject)obj;	
				//				// check the type of return value
				//				System.out.println(jobj.getClass().getName());
				JSONArray topo = (JSONArray)jobj.get("spoutSummary");
//				System.out.println("spout summary "+topo.size());
				for (int i = 0; i<topo.size() ; i++){
					Object objobj = topo.get(i);
					JSONObject jobjjobj = (JSONObject) objobj;
					if(jobjjobj.get("window").equals("600")){
						String templ = (String)jobjjobj.get("completeLatency");
						delay.put(oid, Double.valueOf(templ));
					}
				}
//				topo = (JSONArray)jobj.get("outputStats");	
//				Object objobj = topo.get(0);
//				JSONObject jobjjobj = (JSONObject) objobj;
//				long trans = (Long)jobjjobj.get("transferred");
//				String link = oid+"-";
//				flow.put(link, trans);

				topo = (JSONArray)jobj.get("executorStats");	
				for(int i = 0; i<topo.size(); i++){
					obj = topo.get(i);
					jobj = (JSONObject) obj;
					String host = (String)jobj.get("host");
					Long port = (Long)jobj.get("port");
					String add = host+"-"+String.valueOf(port);
					ArrayList<String> tt;
					if(!exe.containsKey(oid)){
						tt = new ArrayList<String>();
					}
					else{
						tt = exe.get(oid);
					}
					tt.add(add);
					exe.put(oid, tt);
				}

			}
		}

		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
	}



public void operatorDetail(String tid, String oid, ArrayList<String> olist, HashMap<String, Double> exedelay, HashMap<String, Double>  prodelay, 
		HashMap<String, Long> flow, HashMap<String, ArrayList<String>> exe){
	Connect("/api/v1/topology/"+ tid+ "/component/"+ oid);
	try{
		while((output = br.readLine()) != null){
			JSONParser parser = new JSONParser();
			obj = parser.parse(output);
			jobj = (JSONObject)obj;	

			//				// check the type of return value
			//				System.out.println(jobj.getClass().getName());
			JSONArray topo = (JSONArray)jobj.get("boltStats");
			//			    // only allow one spout for each topology
			for (int i = 0; i<topo.size() ; i++){
				Object objobj = topo.get(i);
				JSONObject jobjjobj = (JSONObject) objobj;
				if(jobjjobj.get("window").equals("600")){
					String templ = (String)jobjjobj.get("processLatency");
					prodelay.put(oid, Double.valueOf(templ));
					templ = (String)jobjjobj.get("executeLatency");
					exedelay.put(oid, Double.valueOf(templ));
				}
			}

			topo = (JSONArray)jobj.get("inputStats");	
			for(int i = 0; i<topo.size(); i++){
				Object objobj = topo.get(i);
				JSONObject jobjjobj = (JSONObject) objobj;
				String upo = (String) jobjjobj.get("component");
				if(olist.contains(upo)){
					String link = upo+"-"+oid;
					Long a = (Long) jobjjobj.get("acked");
					flow.put(link, a);
				}
			}

			topo = (JSONArray)jobj.get("executorStats");	
			for(int i = 0; i<topo.size(); i++){
				obj = topo.get(i);
				jobj = (JSONObject) obj;
				String host = (String)jobj.get("host");
				Long port = (Long)jobj.get("port");
				String add = host+"-"+String.valueOf(port);
				ArrayList<String> tt;
				if(!exe.containsKey(oid)){
					tt = new ArrayList<String>();
				}
				else{
					tt = exe.get(oid);
				}
				tt.add(add);
				exe.put(oid, tt);
			}
			////					topologies.get(id).getTworker().put((String)jobj.get("host"), (Long)jobj.get("port"));
			//			//		System.out.println("latency is "+(String)jobj.get("completeLatency"));
			////							topologies.put((String)jobj.get("name"), (String)jobj.get("id"));
			//			    	String spoutid = (String)jobj.get("spoutId");
			//			    	topologies.get(id).setSpout(new Spout(spoutid));
			//			    }
			//			}
		}
	}
	catch(IOException e){
		e.printStackTrace();
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	conn.disconnect();
}

}
