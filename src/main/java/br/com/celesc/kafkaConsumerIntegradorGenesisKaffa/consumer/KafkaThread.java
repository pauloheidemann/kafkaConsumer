package br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaThread {

	private static final Logger logger = LogManager.getLogger(KafkaThread.class);
	
	/**
	 * Método responsável por ficar escutando o tópico ao qual fomos subescritos
	 */
	public void runSingleWorker() {
		try {
			Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
			while (true) {
				ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<Long, String> record : records) {
					ObjectMapper mapper = new ObjectMapper();
					String mensagem = mapper.writeValueAsString(record.value());
					logger.info(mensagem);
					consumer.commitAsync();
				}
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}

}
