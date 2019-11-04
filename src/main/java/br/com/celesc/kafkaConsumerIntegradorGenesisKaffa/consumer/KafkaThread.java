package br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@DependsOn({"consumerCreator"})
public class KafkaThread implements Runnable {

	@Value("${integrador.host}")
	private String integradorHost;
	private static final String INTEGRADOR_OBJETOS_KAFFA_GENESIS = "/integradorObjetosKaffaGenesis/ws/integradorKaffaGenesis";
	private static final Logger logger = LogManager.getLogger(KafkaThread.class);
	
	private Consumer<Long, String> consumer;
	
	private CountDownLatch latch;

	/**
	 * Construtor foi alterado pois a classe tem dependência do bean {@link ConsumerCreator}
	 * Desta forma, toda vez que o Bean {@link KafkaThread} for injetado o Spring já injeta o {@link ConsumerCreator} também
	 * @param creator
	 */
	public KafkaThread(final ConsumerCreator creator) {
		consumer = creator.createConsumer();
		this.latch = new CountDownLatch(1);
	}
	
	@Override
	public synchronized void run() {
		try {
			while (true) {
				ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<Long, String> record : records) {
					logger.info(String.format("Lendo registro do Kafka %s", record.value()));
					ObjectMapper mapper = new ObjectMapper();
					JsonNode jsonRecordValue = mapper.readTree(record.value());
					try {
						sendMessageToIntegrator(jsonRecordValue.get("ID_ATIVIDADE").asText());
						consumer.commitAsync();
					} catch (Exception e) {
						//Se tiver alguma exceção, não comita o offset, o que faz com que o offset seja reprocessado
						logger.info(String.format("Erro ao enviar registro do kafka de ID %s por motivo %s", jsonRecordValue.get("ID_ATIVIDADE").toString(), e.getMessage()));
					}
				}
			}
		} catch (WakeupException we) {
			logger.info("Aplicação foi interrompida", we);
		} catch (Exception e) {
			logger.error("Ocorreu um erro ao ler os tópicos do Kafka", e);
		} finally {
			consumer.close();
			latch.countDown();
		}
	}
	
	/**
	 * Método para interromper o consumer.poll
	 * Vai lançar um WakeUpException no método que está chamando poll do consumer
	 */
	public void shutdown() {
		consumer.wakeup();
	}
	
	/**
	 * Envia o ID da atividade para o integrador </br>
	 * Em caso de sucesso chama o WS do Kaffa para informar que a atividade foi integrada com sucesso </br>
	 * Em caso de erro chama o WS do Kaffa para informar o erro e a mensagem de erro
	 * @param idAtividade
	 * @throws Exception
	 */
	private void sendMessageToIntegrator(String idAtividade) throws Exception {
		try {
			RestTemplate restTemplate = new RestTemplate();
			logger.info(String.format("Enviando para o integrador a atividade de id %s", idAtividade));
			ResponseEntity<String> result = restTemplate.postForEntity(integradorHost + INTEGRADOR_OBJETOS_KAFFA_GENESIS, idAtividade, String.class);
			System.out.println(result);
			//chamar WS do Kaffa para informar que a integração ocorreu sem problema
		} catch (HttpClientErrorException hcee) {
			//chamar WS do kaffa para informar os erros
			logger.error(hcee.getResponseBodyAsString());
			throw hcee;
		} 
	}

}
