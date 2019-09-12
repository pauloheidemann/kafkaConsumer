package kafkaConsumerIntegradorGenesisKaffa.test;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Assert;
import org.junit.Test;

import br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer.ConsumerCreator;
import br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer.KafkaConstants;

public class KafkaTest {

	@Test
	public void consumirMensagens() {
		try {
			Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
			int noMessageFound = 0;
			while (true) {
				ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
				// 1000 is the time in milliseconds consumer will wait if no record is found at
				// broker.
				if (consumerRecords.count() == 0) {
					noMessageFound++;
					if (noMessageFound > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
						// If no message found count is reached to threshold exit loop.
						break;
					else
						continue;
				}
				// print each record.
				consumerRecords.forEach(record -> {
					System.out.println("Record Key " + record.key());
					System.out.println("Record value " + record.value());
					System.out.println("Record partition " + record.partition());
					System.out.println("Record offset " + record.offset());
				});
				// commits the offset of record to broker.
				consumer.commitAsync();
			}
			consumer.close();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

}