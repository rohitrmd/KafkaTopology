package com.storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.storm.kafka.consumer.KafkaConsumer;
import com.storm.kafka.producer.KafkaProducer;

public class KafkaTopology {
	public static void main(String[] args) {
		System.out.println("---------Submitting topology------------");
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-producer", new KafkaProducer());
		builder.setBolt("kafka-consumer", new KafkaConsumer());
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("KafkaTopology", config,
				builder.createTopology());
	}
}
