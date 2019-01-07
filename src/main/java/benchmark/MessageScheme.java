package benchmark;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.jcraft.jsch.Logger;

import bolt.PrinterBolt;

public class MessageScheme implements Scheme{


	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		// TODO Auto-generated method stub
		try{
			
			String msg = new String(ser.array(), "UTF-8");
			return new Values(msg);
		}catch (UnsupportedEncodingException ignored) {
            return null;
        }
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("msg");
	}

}
