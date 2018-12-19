package benchmark;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
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
import bolt.RollingCountStateBolt;
import bolt.TestBolt;

public class SimpleTopology {

	
		// TODO Auto-generated constructor stub

		public static void main(String[] args) throws Exception {
		    
		
		   BrokerHosts brokerHosts = new ZkHosts("43.240.98.29:2181");
	        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "storm", "", "kafkastorm");
	        Config conf = new Config();
	        Properties props = new Properties();
	        props.put("bootstrap.servers", "43.240.98.144:9092");
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        conf.put("kafka.broker.properties", props);
	        conf.put("topic", "storm");
	        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("msgKafkaSpout", new KafkaSpout(spoutConfig));
	        
	        builder.setBolt("simpleT", new PrinterBolt()).shuffleGrouping("msgKafkaSpout");
//	        builder.setBolt("counting", new RollingCountStateBolt(60,30)).fieldsGrouping("simpleT", new Fields("location"));
	        builder.setBolt("counting", new RollingCountBolt(60,30)).fieldsGrouping("simpleT", new Fields("location"));
	        builder.setBolt("filtering", new NodeFilterBolt()).shuffleGrouping("simpleT");
	        builder.setBolt("pathG", new PathGenBolt()).shuffleGrouping("filtering");
	        builder.setBolt("pathD", new PathDenBolt()).shuffleGrouping("counting")
	        											.shuffleGrouping("pathG");
	        
//	        builder.setBolt("pathAD", new PathIdeBolt()).shuffleGrouping("pathD");
	        builder.setBolt("pathV", new PathVelBolt()).shuffleGrouping("pathD");
	        builder.setBolt("pathT", new PathTimingBolt()).shuffleGrouping("pathV");
	        builder.setBolt("timeLog", new LogTimeBolt()).shuffleGrouping("pathT");
//	        builder.setBolt("test", new TestBolt()).shuffleGrouping("pathD");
	        
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
//	            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	       else if (args.length == 0) {
	            String topologyName = "kafkaTopicTopology";
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology(topologyName, conf, builder.createTopology());
	            Utils.sleep(15*60*1000);
	            cluster.killTopology(topologyName);
	            cluster.shutdown();
	       }
		}
	}
