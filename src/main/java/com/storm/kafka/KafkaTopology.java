package com.storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.storm.kafka.producer.KafkaProducer;

public class KafkaTopology {
	public static void main(String[] args) {
		System.out.println("---------Submitting topology------------");
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout", new KafkaProducer());
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("KafkaTopology", config,
				builder.createTopology());
	}
}
