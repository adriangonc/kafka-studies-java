package kafkastudies;

import java.time.LocalDate;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import kafkastudies.util.config.ProjectConfigKafka;

public class SendKafka {
	public static void main(String[] args) {
		producerKafka();
	}

	private static void producerKafka() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProjectConfigKafka.KAFKA_SERVER);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(ProjectConfigKafka.KAFKA_TOPIC, "Hello World!" + LocalDate.now());
			producer.send(record);
		}
	}
}
