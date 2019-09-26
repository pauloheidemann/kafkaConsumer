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
			kafkaThread.runSingleWorker();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

}