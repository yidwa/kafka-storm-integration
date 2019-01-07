package serverlisten;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class Client {

	public static void main(String[] args) {
		Socket socket;
		try {
			socket = new Socket("127.0.0.1", 9999);
		
		DataOutputStream os = new DataOutputStream(socket.getOutputStream());
		String s = "hello world\n world hello\n hello hello";
		OutputStreamWriter osw = new OutputStreamWriter(os);
		osw.write(s);
		osw.flush();
		os.flush();
		os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
