package benchmark;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StaticHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.shade.org.eclipse.jetty.util.log.Log;
import org.apache.storm.spout.MultiScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import bolt.LogTimeBolt;
import bolt.NodeFilterBolt;
import bolt.PathDenBolt;
import bolt.PathGenBolt;
import bolt.PathIdeBolt;
import bolt.PathTimingBolt;
import bolt.PathVelBolt;
import bolt.PrinterBolt;
import bolt.RollingCountBolt;

import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.storm.kafka.spout.KafkaSpoutConfig;
//import org.apache.storm.kafka.spout.KafkaSpout;
import kafka.server.KafkaConfig;

public class KafkaSpoutTopology {
	 public static void main(String[] args) throws Exception {
	        BrokerHosts brokerHosts = new ZkHosts("43.240.98.29:2181");
	        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "storm", "/kafkastorm", "kafkastorm");
	        Config conf = new Config();
	        Properties props = new Properties();
	        props.put("bootstrap.servers", "43.240.97.192:9092");
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        conf.put("kafka.broker.properties", props);
	        conf.put("topic", "storm");
	        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("msgKafkaSpout", new KafkaSpout(spoutConfig));
	        builder.setBolt("simpleT", new PrinterBolt()).shuffleGrouping("msgKafkaSpout");
//	        builder.setBolt("msgKafkaBolt", new PrinterBolt()).shuffleGrouping("msgKafkaSpout");
//	        builder.setBolt("counting", new RollingCountBolt(20,10)).fieldsGrouping("simpleT", new Fields("location"));
//	        builder.setBolt("filtering", new NodeFilterBolt()).shuffleGrouping("simpleT");
//	        builder.setBolt("pathG", new PathGenBolt()).shuffleGrouping("filtering");
////	        builder.setBolt("pathIde", new PathIdeBolt()).shuffleGrouping("pathG");
//	        builder.setBolt("pathD", new PathDenBolt()).shuffleGrouping("counting");
////	        BoltDeclarer bd = builder.setBolt("pathV", new PathVelBolt());
////	        bd.fieldsGrouping("pathG", "stream1", new Fields("path"));
////	        bd.fieldsGrouping("pathD", "stream2", new Fields("path"));
//	        builder.setBolt("pathV", new PathVelBolt()).shuffleGrouping("pathG")
//	        				.shuffleGrouping("pathD");
//	        builder.setBolt("pathT", new PathTimingBolt()).shuffleGrouping("pathV");
//	        builder.setBolt("timeLog", new LogTimeBolt()).shuffleGrouping("pathT");
//	     					.fieldsGrouping("pathD", "streamD", new Fields("route"));
//	        if (args.length == 0) {
//	            String topologyName = "kafkaTopicTopology";
//	            LocalCluster cluster = new LocalCluster();
//	            cluster.submitTopology(topologyName, conf, builder.createTopology());
//	            Utils.sleep(100000);
//	            cluster.killTopology(topologyName);
//	            cluster.shutdown();
//	        } else {
	        conf.setNumWorkers(4);
	        if(args.length>0){
	     	   conf.setNumWorkers(Integer.valueOf(args[0]));
	     	   StormRunner.runTopologyRemotely(builder.createTopology(), args[1], conf);
	        }
//	             StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	        else if (args.length == 0) {
	             String topologyName = "kafkaTopicTopology";
	             LocalCluster cluster = new LocalCluster();
	             cluster.submitTopology(topologyName, conf, builder.createTopology());
	             Utils.sleep(100000);
	             cluster.killTopology(topologyName);
	             cluster.shutdown();
	        }
	        }
	    
	
	 
	 
	    
//	    if (runLocally) {
//	      LOG.info("Running in local mode");
//	      StormRunner.runTopologyLocally(bt.builder.createTopology(), topologyname, bt.topologyConfig, bt.runtimeInSeconds);
//	    }
//	    else {
//	      LOG.info("Running in remote (cluster) mode");
//	      StormRunner.runTopologyRemotely(bt.builder.createTopology(), topologyname, bt.topologyConfig);
//	    }
//	}
//
//}
//		private final BrokerHosts bhosts;
//		public KafkaSpoutTopology(String kzoo){
//			bhosts = new ZkHosts(kzoo);
//		}
	 
//		public StormTopology buildTopology(){
//			SpoutConfig kcon = new SpoutConfig(bhosts, "test", "/kafkastorm", "kafka-storm-testing");
//			kcon.scheme = new SchemeAsMultiScheme(new StringScheme());
//			kcon.useStartOffsetTimeIfOffsetOutOfRange = true;
//			
//			TopologyBuilder builder = new TopologyBuilder();
////			KafkaSpoutConfig ksc = KafkaSpoutConfig.builder("43.240.97.192:9092","test").setGroupId("yeewa")
////										.setEmitNullTuples(false).build();
//			
//			builder.setSpout("kafka-spout", new KafkaSpout(kcon));
////			builder.setSpout("emit", new KafkaSpout(kcon),4);
//			builder.setBolt("print", new PrinterBolt());
//			return builder.createTopology();
//		}
		
		
//	public static void main(String[] args) {
//
////		String nimHost = "115.146.86.60"; 
//		String kz = "137.92.56.139:2181";
//		
//		KafkaSpoutTopology kt = new KafkaSpoutTopology(kz);
//		
//		Config config = new Config();
//		Properties props = new Properties();
//	        
//	     props.put("metadata.broker.list", "43.240.97.192:9092");
//	     props.put("serializer.class", "kafka.serializer.StringEncoder");
//	     config.put("kafka.broker.properties", props);
//	     config.put("topic", "test");
//		 config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
//		
//		StormTopology st = kt.buildTopology();
//
//		try{
////				config.setNumWorkers(2);
////				config.setMaxTaskParallelism(2);
//			LocalCluster lc = new LocalCluster();
//			lc.submitTopology("kafka-testing", config , st);
//				
////			StormSubmitter.submitTopology("kafka-testing", config , st);
//			
//		}
//		catch(Exception e){
//			e.printStackTrace();
//		}
//	}
	 public static void writeFile(String sen){
			try {
//				String path = "/home/ubuntu/Records.txt";
				String path = "/Users/yidwa/Desktop/copying.txt";
//				String path = "/home/ubuntu/TopologyResult.txt";
				File f = new File(path);
				FileWriter fw = new FileWriter(f,true);
				fw.write(sen+"\n");
			
				fw.flush();
					
				fw.close();
				}
				catch (IOException e1) {
						// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		}
	
}
