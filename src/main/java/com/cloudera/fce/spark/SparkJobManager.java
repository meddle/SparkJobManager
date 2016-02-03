package com.cloudera.fce.spark;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;
//import org.apache.kafka.log4jappender.KafkaLog4jAppender;
import com.cloudera.fce.management.Job;
import com.cloudera.fce.management.JobDecoder;
import com.cloudera.fce.management.JobManager;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;





public class SparkJobManager implements JobManager {
	ConsumerConnector consumer;
	String zookeeperURL = "localhost:2181";
	ConsumerConfig consumerConfig;
	KafkaStream<String, Job> stream;
	Integer allowedKafkaErrors = 10;
	protected static final Logger logger = Logger.getLogger(kafka.producer.KafkaLog4jAppender.class);
	Properties props;
	String topic;


	public SparkJobManager(String topic, String groupId, String zookeeperURL) {
		props = new Properties();
		this.zookeeperURL = zookeeperURL;
		this.setTopic(topic);
		props.put("zookeeper.connect", zookeeperURL);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		
		
		consumerConfig = new ConsumerConfig(props);
		HashMap<String,Integer> topicMap = new HashMap<String,Integer>();
		topicMap.put( this.getTopic(), 1);
		consumer=Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String, List<KafkaStream<String, Job>>> consumerMap = 
				consumer.createMessageStreams(topicMap, new StringDecoder(null), 
						new JobDecoder());
		stream = consumerMap.get(this.getTopic()).get(0);
	}
	
	/*public void connect() {
		//TODO: Move Zookeeper Host to Constructor, no need for Connect
		//TODO: Consider make a config class
		//HashMap<String,Integer> topicMap = new HashMap< String,Integer>();
		consumerConfig = new ConsumerConfig(props);
		HashMap<String,Integer> topicMap = new HashMap<String,Integer>();
		topicMap.put( this.getTopic(), 1);
		System.out.println(topicMap);
		consumer=Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String, List<KafkaStream<String, Job>>> consumerMap = 
				consumer.createMessageStreams(topicMap, new StringDecoder(null), 
						new JobDecoder());
		stream = consumerMap.get(this.getTopic()).get(0);
	}*/

	@Override
	public Job getJob() {
		MessageAndMetadata<String, Job> rawMsg;
		Integer kafkaErrors = 0;
		ConsumerIterator<String, Job> consumerIterator = stream.iterator();
		boolean shutdown = false;
		while (!shutdown) {
			try {
				rawMsg = consumerIterator.next();
				kafkaErrors = 0;
			} catch (RuntimeException e) {
				logger.error("Unable to pull message from Kafka", e);
				kafkaErrors++;
				if (kafkaErrors >= allowedKafkaErrors) {
					shutdown = true;
				}
				return null;
			}
			consumer.commitOffsets();
			return((Job)rawMsg.message());
			

		}
		return null;
	}



	@Override
	public Job waitForJob() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void markComplete(Job job) {
		// TODO Auto-generated method stub

	}

	public String getZookeeperURL() {
		return zookeeperURL;
	}

	public void setZookeeperURL(String zookeeperURL) {
		this.zookeeperURL = zookeeperURL;
		props.put("zookeeper.connect", this.getZookeeperURL());
		consumerConfig = new ConsumerConfig(props);
	}

	public ConsumerConfig getConsumerConfig() {
		return consumerConfig;
	}

	public void setConsumerConfig(ConsumerConfig consumerConfig) {
		this.consumerConfig = consumerConfig;
	}

	public Integer getAllowedKafkaErrors() {
		return allowedKafkaErrors;
	}

	public void setAllowedKafkaErrors(Integer allowedKafkaErrors) {
		this.allowedKafkaErrors = allowedKafkaErrors;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

}
