package stormuiinfo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import scala.Array;

public class Starting {

	public static String path = "/Users/yidwa/Desktop/info/";
	public Starting() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws InterruptedException {
		 ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(15);
		 //the times to monitor
		 int count =5;
//		 int count =30;
//		 int metiscount = 1;
		 HashMap<String, HashMap<String, Long>> flowrecords = new HashMap<>();
		 for (int i = 0; i< count; i++){
		 //
			ReUpdate ru = new ReUpdate("http://43.240.96.30:8080",flowrecords);
			scheduledPool.schedule(ru, 0, TimeUnit.SECONDS);
			System.out.println("new thread start");
//			Thread.sleep(10*1000);
//		
//			Starting s = new Starting();
////			s.mergeInfo();
//			s.writeOpdemand();
			Thread.sleep(30*1000);
//			Thread.sleep(2*60*1000);
//			if(i<metiscount){
//				System.out.println("start the metis generation");
//				GenerateMetis gm = new GenerateMetis();
//				scheduledPool.schedule(gm, 0, TimeUnit.SECONDS);
//			}
		 }
		 scheduledPool.shutdown();
		
		 while(!scheduledPool.isTerminated()){
		 }
		
		 System.out.println("all finished");
//	
	}

//	public void mergeInfo() {
//		File f;
//		BufferedReader br;
//		String[] flist = new String[] { "s1", "s2", "s3", "m1", "m2", "m3", "l1", "l2", "l3" };
//		String info = "";
//		try {
//			for (String s : flist) {
//
//				f = new File(path + s);
//				br = new BufferedReader(new FileReader(f));
//				String line = br.readLine();
//				info += s+",";
//				while (line != null) {
//					line = br.readLine();
//					if(line!=null && line.contains(",")){
//						String[] temp = line.split(",\\s");
//						info += temp[0] + "," + temp[1]+",";
//					}
//					else if(line!=null)
//						info += line;
//				}
//				br.close();
//				info += "\n";
//			}
//				info += "\n";
//
//			
//
//				File ff = new File(path+"merge");
//				BufferedWriter bw = new BufferedWriter(new FileWriter(ff, true));
//				bw.write(info);
//				bw.flush();
//				bw.close();
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

	public void writeOpdemand(){
		HashMap<String, ArrayList<String>> exe = new HashMap<String, ArrayList<String>>();
		HashMap<String, String> exedemand = new HashMap<>();
		try {
			File f = new File(path+"performance/execution");
			f.createNewFile();
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
			line = br.readLine();
			while(line!=null){
				String[] temp = line.split(",");
				ArrayList<String> templist = new ArrayList<>();
				for(int i = 1; i<temp.length;i++)
					templist.add(temp[i]);
				if(temp.length>1 && !templist.isEmpty()){
					exe.put(temp[0], templist);
			}	
				line = br.readLine();
			}
			br.close();

			String linese;
			f = new File(path+"merge");
			f.createNewFile();
			br = new BufferedReader(new FileReader(f));
			linese = br.readLine();
			while(linese != null){
				String[] temp = linese.split(",");
				if(temp.length>1){
					for(int i=1; i<temp.length-1; i+=2){
						String add = temp[0]+"-"+temp[i];
						String dem = temp[i+1];
						exedemand.put(add, dem);
					}
				}
				linese = br.readLine();

			}
			br.close();
			
		
			String result="";
			if(exe.size()>0 && exedemand.size()>0){
				for(String s: exe.keySet()){
					result += s+" ";
					double demand = 0;
					for(String ss: exe.get(s)){
						if(exedemand.containsKey(ss))
							demand += Double.valueOf(exedemand.get(ss));
					}
					result += String.valueOf(demand);
					result += "\n";
				}
			}

			f = new File(path+"performance/optdemand");
			f.createNewFile();
			BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
			bw.write(result);
			bw.flush();
			bw.close();


		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}






	}
}
