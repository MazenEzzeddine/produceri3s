import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
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


    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        rnd = new Random();
        config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        producer = new KafkaProducer<String, Customer>(props);
        startServer();
        PrometheusUtils.initPrometheus();

        KafkaClientMetrics producerKafkaMetrics = new KafkaClientMetrics(producer);

        producerKafkaMetrics.bindTo(PrometheusUtils.prometheusRegistry);

       // Worktwosine.startWorkload();

       // WorktPareto.startWorkload();


       // WorkParetoSingle.startWorkload();


        WorkUniform.startWorkload();








        //startServer();

       // Work2.startWorkload();
       // OldWorkload.startWorkload();
        //OldWorkload.startWorkloadUniform();


      //  OldWorkload.startWorkloadUniformBatch50();

       // OldWorkload.startWorkloadUniformBatch25();


        // OldWorkloadSkewed.startWorkload();
        //ConstantWorkload.startWorkload();
    }

    private static void startServer() {
        Thread server = new Thread  (new ServerThread());
        server.start();
    }
}