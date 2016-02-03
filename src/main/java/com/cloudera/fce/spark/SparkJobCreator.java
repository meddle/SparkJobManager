package com.cloudera.fce.spark;

//import java.util.Map;
import java.util.Properties;

import com.cloudera.fce.management.Job;
import com.cloudera.fce.management.JobCreator;
/*import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;*/


/*import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;*/

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;



public class SparkJobCreator implements JobCreator {

	private String zooKeeperHost = "localhost:2181";
	private String brokers = "localhost:9092";
	private Properties properties = new Properties();
	//private ProducerConfig producerConfig = null;
	//private Producer<String,Job> producer = null;
	private KafkaProducer<String, Job> kafkaProducer = null;



	@Override
	public void sendJob(Job job, String topic) {
		//String json = null;
		try {

			//KeyedMessage<String, Job> data = new KeyedMessage<String, Job>(topic, job);
			//producer.send(data);
			//producer.close();
			kafkaProducer.send(new ProducerRecord<String, Job>(topic,
		              job.getJobId(),
		              job)).get();
		} catch (Exception e) {
			//need to chat with Nielsen on how they would like to handle exceptions
			e.printStackTrace();
		}
	}

	public SparkJobCreator(String brokers) {
		//properties.put("metadata.broker.list",this.getBrokers());
		properties.put("bootstrap.servers",brokers);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    properties.put("value.serializer", "com.cloudera.fce.management.JobEncoder");
		//properties.put("serializer.class","com.cloudera.fce.management.JobEncoder");
		properties.put("acks", "1");
		//producerConfig = new ProducerConfig(properties);
		this.brokers = brokers;
		kafkaProducer = new KafkaProducer<String, Job>(this.properties);
	}

	/*private String toJson(Job job) throws JsonProcessingException {
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		String json = ow.writeValueAsString(job);
		return json;
	}*/

	public String getZooKeeperHost() {
		return zooKeeperHost;
	}

	public void setZooKeeperHost(String zooKeeperHost) {
		this.zooKeeperHost = zooKeeperHost;
	}

	@Override
	public void connect() {
		//producer = new Producer<String, Job>(producerConfig);
		kafkaProducer = new KafkaProducer<String, Job>(this.properties);
	}

	@Override
	public void disconnect() {
		kafkaProducer.close();
	}

	public String getBrokers() {
		return brokers;
	}

	public void setBrokers(String brokerList) {
		this.brokers = brokerList;
		properties.put("metadata.broker.list",this.getBrokers());
		//producerConfig = new ProducerConfig(properties);
	}
}
