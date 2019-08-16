import kafka.Kafka;
import kafka.cluster.Cluster;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;
import java.util.UUID;

public class StormWithKafka {
    //Constraints
    final static String
            TOPOLOGY_NAME = "StormWithKafka",//topology name
            ZK_CONN = "localhost:2181",//zookeeper connection
            TOPIC_INFO_GENERATOR = "info",//topic info generator name
            TOPIC_FINAL_INFO = "final-info",//topic final info name
            SPOUT_KAFKA = "kafka-spout",//kafka spout
            BOLT_SPLIT = "split-bolt",//split bolt
            BOLT_COUNT = "count-bolt",//count bolt
            BOLT_KAFKA = "kafka-bolt";//kafka bolt;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder topology = new TopologyBuilder();

        //Set spout
        topology.setSpout(SPOUT_KAFKA, getKafkaSpout(),1);

        //Set split bolt
        topology.setBolt(BOLT_SPLIT, new SplitBolt()).shuffleGrouping(SPOUT_KAFKA);

        //Set word count bolt
        topology.setBolt(BOLT_COUNT, new CountBolt()).shuffleGrouping(BOLT_SPLIT);

        //Set kafka bolt to receive final info
        topology.setBolt(BOLT_KAFKA, getKafkaBolt()).shuffleGrouping(BOLT_COUNT);

        //Instance storm submitter
        LocalCluster stormSubmitter = new LocalCluster();

        //Submit topology
        stormSubmitter.submitTopology(TOPOLOGY_NAME, config, topology.createTopology());

        // Thread.sleep(10000);
        //stormSubmitter.shutdown();

    }

    public static KafkaSpout getKafkaSpout() {
        //Kafka config
        BrokerHosts hosts = new ZkHosts(ZK_CONN);
        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, TOPIC_INFO_GENERATOR, "/" + TOPIC_INFO_GENERATOR, UUID.randomUUID().toString());
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.ignoreZkOffsets = true;
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return new KafkaSpout(kafkaSpoutConfig);
    }

    public static KafkaBolt getKafkaBolt() {
        //set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaBolt()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(TOPIC_FINAL_INFO))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "wordCount"));

    }
} 
