package com.storm.kafka.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

import scala.collection.Seq;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;

public class KafkaProducer extends BaseRichSpout {

	private Producer<String, String> producer;
	private SpoutOutputCollector collector;
	private InputStream in;
	private BufferedReader bufferedReader;

	public void nextTuple() {
		String str = null;
		try {
			while ((str = bufferedReader.readLine()) != null) {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", str);
				producer.send(data);
				System.out.println("Data produced: "+str);
			}
			producer.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void open(Map map, TopologyContext context,
			SpoutOutputCollector _collector) {
		/****
		 * We can provide list of brokers I am using string encoder here. If you
		 * want your class object to be stored in kafka, make it serializable
		 * and store it in serialized (byte[]) format to kafka. You can override
		 * default retention bytes per topic
		 *****/

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:5555");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("retention.bytes", "100000000");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		this.collector = _collector;

		in = ClassLoader.getSystemResourceAsStream("sample.txt");
		bufferedReader = new BufferedReader(new InputStreamReader(in));
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
