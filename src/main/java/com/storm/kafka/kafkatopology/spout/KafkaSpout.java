package com.storm.kafka.kafkatopology.spout;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import clojure.lang.MethodImplCache.Entry;

import com.google.common.collect.ImmutableMap;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.serializer.Decoder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class KafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private ConsumerConnector consumerConnector = null;
	private Map<String, Integer> topicMap;

	public KafkaSpout() {
		topicMap = new HashMap<String, Integer>();
	}

	public void nextTuple() {

	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		topicMap.put("test", 1);
		Properties props = new Properties();
		props.put("zk.connect", "localhost:2181");
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		consumerConnector = Consumer
				.createJavaConsumerConnector(consumerConfig);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector.createMessageStreams(topicMap);
		Iterator<java.util.Map.Entry<String, List<KafkaStream<byte[], byte[]>>>> it = consumerStreams.entrySet().iterator();
		while(it.hasNext())
		{
			java.util.Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry = it.next();
			List<KafkaStream<byte[], byte[]>> list = entry.getValue();
			System.out.println(list.get(0).mkString());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
