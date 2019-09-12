package br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer;

/**
 * 
 * Classe com as constantes de configuração do Kafka
 * @author Paulo Henrique Heidemann
 *
 */
public interface KafkaConstants {

    public static String KAFKA_BROKERS = "localhost:9092";
    public static Integer MESSAGE_COUNT=1000;
    public static String CLIENT_ID="client1";
    public static String TOPIC_NAME="genesis-kaffa";
    public static String GROUP_ID_CONFIG="genesis-kaffa-01";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=5;
    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";
    public static Integer MAX_POLL_RECORDS=1;
	
}
