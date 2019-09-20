package br.com.celesc.kafkaConsumerIntegradorGenesisKaffa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.RestTemplate;

import br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer.KafkaThread;

@SpringBootApplication
@ComponentScan("br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer")
public class KafkaGenesisKaffaApplication extends SpringBootServletInitializer implements CommandLineRunner {

	@Autowired
	private KafkaThread kafkaThread;
	
	private static final Logger logger = LogManager.getLogger(KafkaThread.class);

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
    	return builder.sources(KafkaGenesisKaffaApplication.class);
    }
    
    @Bean
	public RestTemplate restTemplate(RestTemplateBuilder builder) {
		return builder.build();
	}
    
    @Override
    public void run(String... args) throws Exception {
    	/*
         * Creating a thread to listen to the kafka topic
         */
        Thread kafkaConsumerThread = new Thread(() -> {
            logger.info("Starting Kafka consumer thread.");
            kafkaThread.runSingleWorker();
        });
        
        kafkaConsumerThread.start();
    }
	
}
