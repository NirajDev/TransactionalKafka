package com.example.demo.test;

import static java.util.Collections.singleton;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionalConsumer {

	private static String kafka_brokers = "localhost:9092";

	private static String transTopic = "TransEvent";

	private static String transGroupId = "TransGroup";

	public static void main(String[] args) {

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps());

		consumer.subscribe(singleton(transTopic));

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("topic = %s, partition = %s, offset = %d, Key = %s, Message= %s\n",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
				}
				consumer.commitAsync();
			}
		} catch (Exception e) {
			log.error("Unexpected error", e);
		} finally {
			try {
				consumer.commitSync();
			} finally {
				log.error("Closing Consumer");
				consumer.close();
			}
		}
	}

	private static Properties getConsumerProps() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_brokers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, transGroupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}
}
