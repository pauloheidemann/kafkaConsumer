package br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaThread {

	private static final String INTEGRADOR_OBJETOS_KAFFA_GENESIS = "http://localhost:8080/integradorObjetosKaffaGenesis/ws/integradorKaffaGenesis";
	private static final Logger logger = LogManager.getLogger(KafkaThread.class);
	
	/**
	 * Método responsável por ficar escutando o tópico ao qual fomos subescritos
	 * Pega o resultado de cada record e chama o integrador no sentido Kaffa-Genesis
	 */
	public void runSingleWorker() {
		try {
			Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
			while (true) {
				ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(60000));
				for (ConsumerRecord<Long, String> record : records) {
					logger.info(String.format("Lendo registro do Kafka %s", record.value()));
					ObjectMapper mapper = new ObjectMapper();
					JsonNode jsonRecordValue = mapper.readTree(record.value());
					try {
						sendMessageToIntegrator(jsonRecordValue.get("ID").toString());
						consumer.commitAsync();
					} catch (Exception e) {
						//Se tiver alguma exceção, não comita o offset, o que faz com que o offset seja reprocessado
						logger.info(String.format("Erro ao enviar registro do kafka de ID %s por motivo %s", jsonRecordValue.get("ID").toString(), e.getMessage()));
					}
				}
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * Envia o ID da atividade para o integrador </br>
	 * Em caso de sucesso chama o WS do Kaffa para informar que a atividade foi integrada com sucesso </br>
	 * Em caso de erro chama o WS do Kaffa para informar o erro e a mensagem de erro
	 * @param idAtividade
	 * @throws Exception
	 */
	private void sendMessageToIntegrator(String idAtividade) throws Exception {
		RestTemplate restTemplate = new RestTemplate();
		logger.info(String.format("Enviando para o integrador a atividade de id %s", idAtividade));
		ResponseEntity<String> result = restTemplate.postForEntity(INTEGRADOR_OBJETOS_KAFFA_GENESIS, idAtividade, String.class);
		if(result.getStatusCode().equals(HttpStatus.CREATED) || result.getStatusCode().equals(HttpStatus.OK))
			//TODO chamar WS do Kaffa para confirmar a integração da atividade
			System.out.println(result);
		else
			//TODO chamar WS do Kaffa para informar erro e repassar motivo do erro
			System.out.println(result);
	}

}
