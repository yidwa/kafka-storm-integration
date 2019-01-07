package scheduling;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.EvenScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.Component;

public class Netscheduling implements IScheduler{

	public void prepare(Map conf) {}

	public void schedule(Topologies topologies, Cluster cluster) {
		System.out.println("Scheduler: begin network scheduling");

		Collection<TopologyDetails> td;
		td = topologies.getTopologies();
		Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();

		for(TopologyDetails topology :td){
			if (topology != null) {
				boolean needsScheduling = cluster.needsScheduling(topology);
				
				// the list of chosen hosts
				ArrayList<String> candidate = getCandidate();
				boolean reScheduling = false;
//				System.out.print("current assigned to host :");
				
//				System.out.println();
				
				if (!needsScheduling) {
					System.out.println("it is not a new topology");
					SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
					Set<WorkerSlot> workers = currentAssignment.getSlots();
					Set<String> assignedhost = new HashSet<String>();
					for(WorkerSlot wa: workers){
						String h = cluster.getHost(wa.getNodeId());
						assignedhost.add(h);
					}
					
					
					
//					System.out.println("candidates are ");
//					for(String s: candidate){
//						System.out.println(s);
//					}
					
					for(String s: assignedhost){
						if(!candidate.contains(s)){
							reScheduling = true;
							System.out.println(s+" is not inlcuded in the candidate");
						}
//						System.out.print(s+" , ");
					}
				
					if(!reScheduling)
						System.out.println(topology.getName() + " DOES NOT NEED scheduling.");
					else{
						System.out.println(" needs rescheduling "+reScheduling);
						System.out.println("start scheduling for "+topology.getName());
						netSchedule(cluster, topology, supervisors, candidate);
					}
				} 	
				else {
					System.out.println(topology.getName()+" needs scheduling.");
					System.out.println("start scheduling for "+topology.getName());
					netSchedule(cluster, topology, supervisors, candidate);
				}
			}
		}
		new EvenScheduler().schedule(topologies, cluster);


	}

	public ArrayList<String> getCandidate(){
		ArrayList<String> candidate = new ArrayList<>();
		ArrayList<String> temp = readCluster();
//		System.out.println("read cluster "+temp.size());
		String chosthost = getLAva(temp);
		String[] hosts = chosthost.split(" ");
		for(String s: hosts){
			candidate.add(s);
		}
		return candidate;
	}

	public void netSchedule(Cluster cluster, TopologyDetails topology, Collection<SupervisorDetails> supervisors, ArrayList<String> candidate){
		// find out all the needs-scheduling components of this topology
//		Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
		Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap();
		Map<String, Component> compos = topology.getComponents();
		for(String s: compos.keySet()){
			componentToExecutors.put(s, compos.get(s).execs);
		}
		
		ArrayList<SupervisorDetails> Supervisors = new ArrayList<>();
//		// the list of chosen hosts
//		ArrayList<String> candidate = getCandidate();
//		
//		System.out.println("candidate  "+candidate.toString());
		
		for (SupervisorDetails supervisor : supervisors) {
//			System.out.println(supervisor.getHost());
//			if (meta != null && meta.get("site") != null){
//				System.out.println("meta "+meta.get("site"));
			if (candidate.contains(supervisor.getHost())){
					Supervisors.add(supervisor);
				}
		}

		ArrayList<ArrayList<String>> popt = getPartition();

		HashMap<String, Integer> opsmap = opsMap(popt);
//		System.out.println("osmap size "+opsmap.size());

		// the key is the name of the operator and the value is the list of executors
		HashMap<String, List<ExecutorDetails>> executorinfo = new HashMap<>();


		//		//iterate the operators to add the executors
	  if(!componentToExecutors.isEmpty()){
		for(String s: componentToExecutors.keySet()){
//			System.out.println("executor for "+s);
			if(opsmap.containsKey(s)){
				//					System.out.println(s+" , is contained in the "+ i +" th partition");
				List<ExecutorDetails> executors = componentToExecutors.get(s);
				executorinfo.put(s, executors);
//				System.out.println("executorinfo put "+s+" , "+ executors.size());
			}
		}

		HashMap<String, SupervisorDetails> sup = new HashMap<>();
		for(SupervisorDetails sd : Supervisors){
			sup.put(sd.getHost(), sd);
		}

		
		//ordered supervisor
		ArrayList<SupervisorDetails> suplist = orderSup(cluster, sup);
		//
		//
		//
		
		if(!suplist.isEmpty()){	
			
			Set<WorkerSlot> slots = (Set<WorkerSlot>) cluster.getUsedSlots();
			
			cluster.freeSlots(slots);
			//			for(int i = 0; i<suplist.size(); i++){
			//				SupervisorDetails des = suplist.get(i);
			//				
			//			}
			//iterate for each parition
			int j = 0;
			for(int i = 0; i<3; i++){
				
				for(String s: opsmap.keySet()){
					if(opsmap.get(s) == i){
						System.out.println("now assign operators for "+s);
						List<ExecutorDetails> exe = executorinfo.get(s);
//						System.out.println("exe size is "+exe.size());
						SupervisorDetails sd = suplist.get(j);
						System.out.println("get the supervisor "+sd.getHost());
						List<WorkerSlot> availableSlots = cluster.getAvailableSlots(sd);
						
						if(availableSlots.isEmpty() && !exe.isEmpty()) {
							System.out.println("no avaialble slot left in "+sd.getHost());
							j++;
						}
						else if(exe.isEmpty()){
							System.out.println("no executors need to assign");
						}
						else{
							System.out.println(sd.getHost()+" has "+availableSlots.size()+" slots");
							WorkerSlot ws = availableSlots.get(0);
							System.out.println("node is "+ws.getNodeId());
							System.out.println("executor size is "+exe.size());
							cluster.assign(availableSlots.get(0), topology.getId(), exe);
							System.out.println("Assign "+s +" operators to "+ sd.getHost());
						}
					}
				}
				j++;
			}
			 Map<String, List<ExecutorDetails>> ctE = cluster.getNeedsSchedulingComponentToExecutors(topology);
			 SupervisorDetails sd = suplist.get(j);
			 for(String s : ctE.keySet()){
				 List<WorkerSlot> availableSlots = cluster.getAvailableSlots(sd);
				 cluster.assign(availableSlots.get(0), topology.getId(), ctE.get(s));
			 }
		}
	  }
		else{
			System.out.println("cannot find suitable supervisor or no need to scheduler executor");


		}
	}
	

	public ArrayList<SupervisorDetails> orderSup(Cluster cluster, HashMap<String, SupervisorDetails> sup){
		ArrayList<SupervisorDetails> result = new ArrayList<>();
		HashMap<String, Integer> avai = new HashMap<>();
		for(String supname: sup.keySet()){
			int s = cluster.getAvailableSlots(sup.get(supname)).size();
			avai.put(supname, s);
		}
		Set<Entry<String, Integer>> set = avai.entrySet();
		List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(
				set);
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		for(Entry<String, Integer> entry : list)
			result.add(sup.get(entry.getKey()));
		return result;
	}


	/**
	 * get the cluster host with largest availability
	 * @param clusterhost
	 * @return
	 */
	public String getLAva(ArrayList<String> clusterhost){
		HashMap<String, Double> status = new HashMap();
		String result = "";
		try{
			//			File f = new File("/Users/yidwa/Desktop/info/coordinate");
			File f = new File("/home/ubuntu/info/coordinate");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			while(line!=null){
				String[] temp = line.split(" ");
				if(temp.length>1){
					status.put(temp[0], Double.valueOf(temp[3]));
					//	    			System.out.println("put "+temp[0]+" "+ Double.valueOf(temp[3]));
				}
				line = br.readLine();
			}


			Double[] ava = new Double[3];
			for(int i = 0 ; i<ava.length; i++){
				double a = 0;
//			    System.out.println("cluster get "+i+" is "+clusterhost.get(i));
				String[] temp = clusterhost.get(i).split(" ");
				for(String host : temp){
					//	    			System.out.println("host is "+host);
					a += status.get(host);
				}
				ava[i] = a;
			}

			double maxv = 0;
			int index = 0;
			for(int i = 0; i<ava.length; i++){
				//	    		System.out.println("the avaialblity of "+clusterhost.get(i)+" , "+ava[i]);
				if(ava[i]>maxv){
					maxv = ava[i];
					index = i;
				}
			}

			result = clusterhost.get(index);
		}
		catch(IOException e){
			System.out.println("no coordinate file found");
		}

		return result;
	} 


	public ArrayList<String> readCluster(){
		ArrayList<String> cluster = new ArrayList<>();
		try{
			//			File f = new File("/Users/yidwa/Desktop/info/result");
			File f = new File("/home/ubuntu/info/result");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			while(line!=null){

				String[] temp = line.split(";");
				if(temp.length>1){
					for(int i = 0; i<temp.length; i++){
						cluster.add(temp[i]);
					}
				}
				line = br.readLine();
			}
		}
		catch(IOException e){
			System.out.println("no result file found");
		}
		return cluster;
	}

	public HashMap<String, Integer> opsMap(ArrayList<ArrayList<String>> par){
		HashMap<String, Integer> result = new HashMap<>();
		for(int i = 0; i<par.size(); i++){
			for(String s: par.get(i)){
				result.put(s, i);
			}
		}
		return result;
	}
	public ArrayList<ArrayList<String>> getPartition(){
		ArrayList<ArrayList<String>> result = new ArrayList<>();
		try{
			//			File f = new File("/Users/yidwa/Desktop/info/partition");
			File f = new File("/home/ubuntu/info/partition");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			while(line!=null){
				String[] temp = line.split(" ");
				ArrayList<String> popt = new ArrayList<>();
				if(temp.length>1){
					for(int i = 0; i<temp.length; i++){
						popt.add(temp[i]);
						if(temp[i].equals("simpleT"))
							popt.add("msgKafkaSpout");
					}
				}
				if(popt.size()>0)
					result.add(popt);
				line = br.readLine();
			}
		}
		catch(IOException e){
			System.out.println("no parition file found");
		}
		return result;
	}
	//	public static void main(String[] args) {
	//		Netscheduling ns = new Netscheduling();
	//		ArrayList<ArrayList<String>> temp = ns.getPartition();
	//	    HashMap<String, Integer> map = ns.opsMap(temp);
	//		for(String s: map.keySet())
	//			System.out.println(s+ " , "+map.get(s));
	//		//		}
	//	}
}