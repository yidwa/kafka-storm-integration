package benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import calculating.HMapSortByvalues;


public class KafkaP {

	  private  KafkaProducer<Integer, String> producer;
	  private final Properties properties = new Properties();
	  private String topic;
	  public KafkaP(String topic) {
	        properties.put("bootstrap.servers", "43.240.98.144:9092");
	        properties.put("client.id", "yeewa");
	        properties.put("serializer.class", "kafka.serializer.StringEncoder");
	        properties.put("request.required.acks", "1");
	        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
	        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        producer = new KafkaProducer<>(properties);
	        this.topic = topic;
	    }

	    public void messSend(){
	    	int messageNo =1;
	    	try{
	    	FileInputStream fi = new FileInputStream(new File("/Users/yidwa/Desktop/mel"));
		    BufferedReader br = new BufferedReader(new InputStreamReader(fi));
		    String line = br.readLine();
		    HashMap<String, Integer> records = new HashMap<>();
		    long millis = 0;
		    
	    	while(line!= null){
	          		char a = 'A';
	    			millis = System.currentTimeMillis();
	    			// the emit values for each slot
	    			String[] values = line.split("\\s+");
	    			ArrayList<String> temp = new ArrayList<>();
	    			for(String value : values){
	    				int amount = Integer.valueOf(value);
	    				records.put(String.valueOf(a), amount);
	    				temp.add(String.valueOf(a));
	    				a+=1;
	    			}
	    			//sort the amount of pedestrain for each slot
	    			HashMap<String, Integer> sorted = HMapSortByvalues.sortByValues(records);
	    			
	    			int exe = 0;
	    			Set set = sorted.entrySet();
	    			Iterator iterator = set.iterator();
	    			while(iterator.hasNext()){
	    				Map.Entry me = (Map.Entry) iterator.next();
	    				int count = (Integer)me.getValue();
	    				int rest = count - exe;
	    				if(count>0){
	    					for(int i = 0; i<rest; i++){
	    						for(String value: temp){
	    							producer.send(new ProducerRecord<Integer, String>(topic, messageNo, value)).get();
	    							System.out.println("send value "+value);
	    							messageNo +=1;
	    						}
	    					}
	    					exe = count;
	    				}
	    				temp.remove((String)me.getKey());
	    				}
	    		
	    			//if the time not come to the next minute
//	    			long latemillis = System.currentTimeMillis();
////	    			System.out.println("now after sending out the time is "+latemillis);
//	    		
//	    			if ((latemillis - millis)/1000 <300){
//	    				System.out.println("not yet , the time now is "+latemillis+ " still have " +(latemillis - millis)/1000 +"s left");
//	    				Thread.sleep(300*1000-(latemillis - millis));
//	    			}
//	    			System.out.println("restart at "+System.currentTimeMillis());
	    			line = br.readLine();
	    			}		
	    	}
	    	catch(FileNotFoundException e){
	    		System.out.println("the file is not exist");
	    	}
	    	catch(InterruptedException e){
	    		e.printStackTrace();
	    	} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    public static void main(String[] args) {
	        String topic = "storm";
	        new KafkaP(topic).messSend();;
	        
	}

	class DemoCallBack implements Callback {
		 
	    private final long startTime;
	    private final int key;
	    private final String message;
	 
	    public DemoCallBack(long startTime, int key, String message) {
	        this.startTime = startTime;
	        this.key = key;
	        this.message = message;
	    }
	 
	    /**
	     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
	     * 
	     * @param metadata  The metadata contains the partition and offset of the record. Null if an error occurred.
	     * @param exception The exception thrown during processing of this record. Null if no error occurred.
	     */
	 

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			// TODO Auto-generated method stub
			  long elapsedTime = System.currentTimeMillis() - startTime;
		        if (metadata != null) {
		            System.out.println(
		                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
		                    "), " +
		                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		        } else {
		            exception.printStackTrace();
		        }
		}
	}
}
