package general;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class Method {



	public static void writeFile(String sen, String filename, boolean appendFile){
		try {
			String path = "/Users/yidwa/Desktop/info/"+filename;
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
			System.out.println("file not found");
		}
	}

}
