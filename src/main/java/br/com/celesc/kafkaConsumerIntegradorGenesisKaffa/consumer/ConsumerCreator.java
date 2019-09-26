package br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 
 * Classe para criar o objeto que irá consumir as mensagens do tópico genesis-kafka
 * @author Paulo Henrique Heidemann
 *
 */
@Component
public class ConsumerCreator {

	@Value("${kafka.bootstrap.servers}")
    public String kafkaBroker;
	
	@Value("${integrador.host}")
	private String integradorHost;
	
    public Integer MESSAGE_COUNT=1000;
    public String CLIENT_ID="KAFKA-INTEGRADOR-KAFFA-GENESIS";
    public String TOPIC_NAME="genesis-kaffa-integracao-CHECKOUTS_KAFFA";
    public String GROUP_ID_CONFIG="GENESIS-KAFFA";
    public Integer MAX_NO_MESSAGE_FOUND_COUNT=5;
    public String OFFSET_RESET_LATEST="latest";
    public String OFFSET_RESET_EARLIER="earliest";
    public Integer MAX_POLL_RECORDS=100;
    public Integer MAX_POLL_INTERVAL_MS=300000;

	public Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        return consumer;
	}
	
}