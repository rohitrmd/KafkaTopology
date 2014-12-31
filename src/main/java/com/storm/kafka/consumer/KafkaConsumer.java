package com.storm.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class KafkaConsumer extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private String topic;
	private ConsumerConnector connector;

	public void execute(Tuple arg0) {
	}

	public void prepare(Map map, TopologyContext context,
			OutputCollector collector) {
		/****
		 * To set up kafka consumer: Set up same number of streams as partitions
		 * for the topic. You should use same number of threads to parallely
		 * read from these streams. Right now I will be using only single
		 * thread.
		 */
		this.topic = "test";
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 1);
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "test-group1");
		props.put("auto.offset.reset", "smallest");
		props.put("request.required.acks", 1);

		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		connector = Consumer.createJavaConsumerConnector(consumerConfig);

		ExecutorService executor = Executors.newFixedThreadPool(1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector
				.createMessageStreams(topicMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this.topic);
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new Runnable() {

				public void run() {
					ConsumerIterator<byte[], byte[]> it = stream.iterator();
					while (it.hasNext()) {
						System.out.println("Data consumed: "
								+ new String(it.next().message()));
					}
				}

			});
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}

}
