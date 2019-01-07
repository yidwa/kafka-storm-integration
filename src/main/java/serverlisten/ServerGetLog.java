package serverlisten;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerGetLog {

	public static void main(String[] args) {
		try {
		ServerSocket listener = new ServerSocket(9999);
		
//			File f = new File("/Users/yidwa/Desktop/info/mellog");
			File f = new File("/home/ubuntu/Mel/mellog");
//			FileWriter fw = new FileWriter(f, true);
			
			
			while(true){
				Socket socket = listener.accept();
				System.out.println("receiving request from "+socket.getInetAddress());
				FileWriter fw = new FileWriter(f, true);
					InputStream in = socket.getInputStream();
					Reader reader = new InputStreamReader(in);
					BufferedReader br = new BufferedReader(reader);
					String line = br.readLine();
					while(line!=null){
						System.out.println("receive info "+line);
						fw.write(line);
						fw.write("\n");
						fw.flush();
						line = br.readLine();
					}
				
					fw.close();
					in.close();

				}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}

}
		
