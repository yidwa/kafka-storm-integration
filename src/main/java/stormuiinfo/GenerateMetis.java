package stormuiinfo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.storm.utils.Time;

import general.Method;
import scala.collection.script.Start;

<<<<<<< HEAD
/**
 * metis graph partitioning
 * @author yidwa
 *
 */
=======

>>>>>>> afe951f4e91a815d89cfc2ac675f0c1f7892dfc2

public class GenerateMetis implements Runnable{


	String[] ops;
	ArrayList<ArrayList<Integer>> edges;
	String path;

	public GenerateMetis() {
		// TODO Auto-generated constructor stub
		ops = new String[]{"simpleT","counting","filtering","pathG","pathD","pathV","pathT","timeLog"};
		edges = iniEdges();
		this.path = "/Users/yidwa/Documents/PhD/METIS/metis-5.1.0/";

	}

//	public static void main(String[] args) {
//		GenerateMetis gm = new GenerateMetis();
//		gm.writeDAG("/Users/yidwa/Documents/PhD/METIS/metis-5.1.0/inputfile");
//	}
//	
//	
	
	
	public ArrayList<ArrayList<Integer>> iniEdges(){
		ArrayList<ArrayList<Integer>> edges = new ArrayList<>();
		ArrayList<Integer> temp = new ArrayList<>();
		temp.add(1);
		temp.add(2);
		edges.add(temp);
		temp = new ArrayList<>();
		temp.add(4);
		temp.add(0);
		edges.add(temp);
		temp = new ArrayList<>();
		temp.add(3);
		temp.add(0);
		edges.add(temp);
		temp = new ArrayList<>();
		temp.add(4);
		temp.add(2);
		edges.add(temp);
		temp = new ArrayList<>();
		temp.add(5);
		temp.add(1);
		temp.add(3);
		edges.add(temp);
		temp = new ArrayList<>();
		temp.add(6);
		temp.add(4);
		edges.add(temp);
		temp = new ArrayList<>();
		temp.add(7);
		temp.add(5);
		edges.add(temp);
		temp = new ArrayList<>();
		temp.add(6);
		edges.add(temp);
		return edges;
	}
	public void writeDAG(String path){
		HashMap<String, String> demand = readFile("optdemand");

		HashMap<String, String> flowr = readFile("flow");

		String result = "";
		result += "8 8 011";
		result += "\n";
		for(int i = 0 ; i< ops.length;i++){
			int d = Double.valueOf(demand.get(ops[i])).intValue();
			result += d+" ";

			ArrayList<Integer> t = edges.get(i);
			for(int tt: t){
				String link = "";
				if(i<tt)
					link = ops[i]+"-"+ops[tt];
				else
					link = ops[tt]+"-"+ops[i];

				if(flowr.containsKey(link)){
					String value = flowr.get(link);
					int inc = tt+1;
					result += inc+" "+value+" ";
				}
			}

			result+="\n";
		}
		writeFile(result, path, false);
	}


	public HashMap<String, String> readFile(String filename){
		HashMap<String, String> temphash = new HashMap<String,String>();
		try{
			File f = new File(Starting.path+filename);
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			while(line!=null){
				String[] temp = line.split(" ");
				if(temp.length>1){
					temphash.put(temp[0], temp[1]);
				}
				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return temphash;
	}
	
	
	
	// parition result in the array
	public void getPartition(){
		String result = "";
		ArrayList<Integer> par = new ArrayList<>();
		try{
		    File output = new File(Starting.path+"output");
		    BufferedReader br = new BufferedReader(new FileReader(output));
		    String line = br.readLine();
		    while(line!=null){
		    	par.add(Integer.valueOf(line));
		    	line = br.readLine();
		    }
		 // parition into 3 groups
		    for(int i = 0 ; i<3;  i++){
		    	for(int j = 0 ; j<par.size(); j++){
		    		if(par.get(j) == i)
		    			result += ops[j]+" ";
		    	}
		    	result += "\n";
		    }
		}
		catch(IOException e){
			System.out.println("output file not found");
		}
		Method.writeFile(result, "partition", false);
	}
	
	
	
	public static void writeFile(String sen, String filepath, boolean appendFile){
		try {
			String path = filepath;
//			String path = "/home/ubuntu/Mel/info/"+filename;
			//				String path = "/Users/yidwa/Desktop/CoordinateRecords_" + file +".txt";
			//				String path = "/home/ubuntu/TopologyResult.txt";
			File f = new File(path);
			FileWriter fw = new FileWriter(f,appendFile);
	

			fw.write(sen+"\n");

			fw.flush();

			fw.close();
		}
		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		writeDAG("/Users/yidwa/Documents/PhD/METIS/metis-5.1.0/inputfile");
	
		getPartition();
	}

}
