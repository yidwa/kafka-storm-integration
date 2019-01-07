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

public class Partition {
	
	@SuppressWarnings("resource")
	public static HashMap<String, String[]> getOpt(ArrayList<String> topologyid){
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		try {
			File f = new File(Starting.path+"optReorder");
			BufferedReader br;
			br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			String n = "";
			String[] t = null;
			while (line!=null) {
				if(line.contains("-"))
					n = line;
				else if(line.contains(" ")) {
					t = line.split(" ");
					result.put(n, t);
				}
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	
	public static void writeOpdemand(HashMap<String, String> maxLa, HashMap<String, String[]> opts){
			String result ="";
		try {
			
			for(String s: opts.keySet()) {
				result+=s+"\n";
				for(String opt : opts.get(s)) {
					if(maxLa.containsKey(opt))
						result+= opt+ " "+maxLa.get(opt)+"\n";
				}
				result+="\n";
			}

			File f = new File(Starting.path+"optdemand");
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
