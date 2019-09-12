package br.com.celesc.kafkaConsumerIntegradorGenesisKaffa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;

import br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer.KafkaThread;

@SpringBootApplication
@ComponentScan("br.com.celesc.kafkaConsumerIntegradorGenesisKaffa.consumer")
public class KafkaGenesisKaffaApplication extends SpringBootServletInitializer implements CommandLineRunner {

	@Autowired
	private KafkaThread kafkaThread;
	
	private static final Logger logger = LogManager.getLogger(KafkaThread.class);

    public static void main(String[] args ) {
        SpringApplication.run(KafkaGenesisKaffaApplication.class, args);
    }
    
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
    	return builder.sources(KafkaGenesisKaffaApplication.class);
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
