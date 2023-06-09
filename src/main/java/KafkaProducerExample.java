import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
    private static long iteration = 0;

    static KafkaProducerConfig config;
    static KafkaProducer<String, Customer> producer;
    static Random rnd;
    static long key;
    static int eventsPerSeconds;

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        rnd = new Random();
        config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        producer = new KafkaProducer<String, Customer>(props);
        //log.info("Sending {} messages ...", config.getMessageCount());


        Instant inst = Instant.now();

        while (Duration.between(inst, Instant.now()).getSeconds() < 5 * 60) {

            for (int i = 0; i <250 ; i++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }

            Thread.sleep(1000);


        }
    }
}