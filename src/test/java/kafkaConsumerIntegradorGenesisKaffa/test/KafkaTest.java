package kafkaConsumerIntegradorGenesisKaffa.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;

import br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.KafkaGenesisKaffaApplication;
import br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer.KafkaThread;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = "spring.profiles.active=dev", classes = KafkaGenesisKaffaApplication.class)
@ComponentScan("br.com.celesc.kafkaConsumerIntegradorGenesisKaffa")
public class KafkaTest {
	
	@Autowired
	private KafkaThread kafkaThread;
	
	@Test
	public void consumirMensagens() {
		try {
			/*
	         * Creating a thread to listen to the kafka topic
	         */
	        Thread kafkaConsumerThread = new Thread(() -> {
	            kafkaThread.run();
	        });
	        
	        //inicia a thread
	        kafkaConsumerThread.start();
	        
	        //adiciona um shutdown hook
	        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	        	kafkaThread.shutdown();
	        }));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

}