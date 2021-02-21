package com.example.demo.test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionalProducer {

	private static String kafka_brokers = "localhost:9092";

	private static String transTopic = "TransEvent";

    private static KafkaProducer<String, String> kafkaProducer;
    private static Integer messageCounter = 0;


    private static Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_brokers);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "prod-1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private static void setUpKafkaProducer() {
        kafkaProducer = new KafkaProducer<>(getProducerProps());
        kafkaProducer.initTransactions();
    }


    private static void send(final String key, final String message) throws KafkaException {
    	Future<RecordMetadata> response = kafkaProducer.send(new ProducerRecord<>(transTopic, key, message));
        System.out.println("\t Message Published to Kafka, Key=" + key + ", Message=" + message);
        try {
			RecordMetadata recordMetadata = response.get();
			log.info("response - topic [{}], partition [{}], offset [{}]", Arrays.asList(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()).toArray());

		} catch (InterruptedException e) {
			log.error("Error in sending message");
		} catch (ExecutionException e) {
			log.error("Error in sending message");
		}
    }


    private static void sendTransactionalBatches(final int numOfBatch, final int numOfMsgInBatch) {
        for (int batchCnt = 1; batchCnt <= numOfBatch; batchCnt++) {
            final long beginTime = System.currentTimeMillis();
            kafkaProducer.beginTransaction();
            System.out.println("[Producer] Transaction Started, Batch=" + batchCnt);
            try {
                for (int msgCnt = 1; msgCnt <= numOfMsgInBatch; msgCnt++) {
                    final String key = "[BatchId=" + batchCnt + "][BatchMessageId=" + msgCnt + "]";
                    final String message = "Message-" + messageCounter;
                    send(key, message);
                    messageCounter++;
                }
                kafkaProducer.commitTransaction(); // commit
                final long batchTimeTaken = System.currentTimeMillis() - beginTime;
                System.out.println("[Producer] Transaction Committed, For Batch=" + batchCnt + ", NumOfMessages=" + numOfMsgInBatch + ", Time taken=" + batchTimeTaken);
            } catch (KafkaException kafkaException) {
                kafkaProducer.abortTransaction();
                System.out.println("[Producer] Transaction Aborted, Batch=" + batchCnt);
                kafkaProducer.close();

            }
        }
        System.out.println("\nAll Requested Batches Done, Num Of Batch="+numOfBatch+", numOfMsgInEachBatch="+numOfMsgInBatch);
    }


    //Run the Kafka Transactional Demo
    private static void runKafkaTransactionalProducer() {
        setUpKafkaProducer();
        sendTransactionalBatches(10, 100);
        sendTransactionalBatches(100, 1000);
        sendTransactionalBatches(1000, 10000);
    }

	
	public static void main(String[] args) {

		runKafkaTransactionalProducer();
		
	}

}
