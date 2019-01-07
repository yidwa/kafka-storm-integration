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


public class KafkaPinmin {

	  private  KafkaProducer<Integer, String> producer;
	  private final Properties properties = new Properties();
	  private String topic;
	  public KafkaPinmin(String topic) {
	        properties.put("bootstrap.servers", "43.240.98.144:9092");
	        properties.put("client.id", "yeewa");
	        properties.put("serializer.class", "kafka.serializer.StringEncoder");
	        properties.put("request.required.acks", "1");
	        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
	        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//	        properties.put("request.timeout.ms", 60000);
	        producer = new KafkaProducer<>(properties);
	        this.topic = topic;
	    }

	    public void messSend(){
	    	int messageNo =1;
	    	try{
	    	FileInputStream fi = new FileInputStream(new File("/Users/yidwa/Desktop/mel_peak"));
		    BufferedReader br = new BufferedReader(new InputStreamReader(fi));
		    String line = br.readLine();
		    HashMap<String, Integer> records = new HashMap<>();
		    long millis = 0;
		    System.out.println("line is "+line);
	    	while(line!= null){
	          		char a = 'A';
	    			millis = System.currentTimeMillis() ;
	    			String[] values = line.split("\\s+");
	    			for(String value : values){
	    				int amount = Integer.valueOf(value);
	    				//emit the amount of 10min every 1min, 1 hour data fed within 6 mins
	    				int tempaomount = (Integer) amount/6;
	    				records.put(String.valueOf(a), tempaomount);
	    				//the list of characters
//	    				temp.add(String.valueOf(a));
	    				a+=1;
	    			}
	    			//sort the characters according to their frquencies 
//	    			HashMap<String, Integer> sorted = HMapSortByvalues.sortByValues(records);
	    			
//	    			Iterator iterator = set.iterator();
	    			//iterate 6 times and every 1 minute send out the data received in 10mins
	    			long tempmilli = 0;
	    			for(int i = 0; i<6; i++){
	    				if (i== 0)
	    				     tempmilli = millis;
//	    				if(temp.isEmpty()){
//	    					int k = values.length;
//	    					int internalcount = 0;
//	    					while(internalcount<k){
//	    	    				temp.add(String.valueOf(a));
//	    	    				a+=1;
//	    	    				internalcount++;
//	    	    			}
//	    				}
//	    				Iterator iterator = set.iterator();
	    				for(String s: records.keySet()){
	    					int count = records.get(s);
	    					//the amount of data that supposed to send in this minute
	    				
	    					if(count>0){
	    						System.out.println("will emit "+ count +" "+ s);
	    						for(int j = 0; j<count; j++){
	    							producer.send(new ProducerRecord<Integer, String>(topic, messageNo, s));
	    		
	    						   messageNo +=1;
	    							}
	    						}
	    					
//	    					temp.remove((String)me.getKey());
	    					System.out.println("finish emit for "+ s);
	    				}
	    				long left = System.currentTimeMillis()-tempmilli;
	    				System.out.println("the time left for sending next batch"+left+ " ms");
	    				
	    				
	  
	    				long sle = 60000-left;
	    				if(sle > 0){
	    					System.out.println("need to sleep "+ (sle/1000) +"s");
	    					Thread.sleep(sle);
	    				}
	    				tempmilli = System.currentTimeMillis();
	    			}
	    		
	    			//if the time not come to the next minute
	    			long latemillis = System.currentTimeMillis();
//	    			System.out.println("now after sending out the time is "+latemillis);
	    		
	    			if ((latemillis - millis)/1000 <360){
	    				Thread.sleep(360*1000-(latemillis - millis));
	    			}
	    		
	    			System.out.println("restart now");
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
			} 

	    }
	    public static void main(String[] args) {
	        String topic = "storm";
	        new KafkaPinmin(topic).messSend();;
	        
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
