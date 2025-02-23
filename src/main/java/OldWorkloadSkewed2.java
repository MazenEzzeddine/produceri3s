import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.UUID;

public class OldWorkloadSkewed2 {
    static float ArrivalRate;
    public static void  startWorkload() throws IOException, URISyntaxException, InterruptedException {

        final Logger log = LogManager.getLogger(OldWorkloadSkewed2.class);
        Workload wrld = new Workload();
        double first3Partitions = 0.0;
        double remaining2Partitions = 0.0;

        Random rnd = new Random();
        // over all the workload
        for (int i = 0; i < wrld.getDatax().size(); i++) {
            log.info("sending a batch of authorizations of size:{}",
                    Math.ceil(wrld.getDatay().get(i)));
            ArrivalRate = (float) Math.ceil(wrld.getDatay().get(i));

            first3Partitions = ArrivalRate * 0.8;
            remaining2Partitions = ArrivalRate * 0.2;

            //   sendToFirst 3 partitions
            for (long j = 0; j < first3Partitions/3.0; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
               KafkaProducerExample.
                       producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        0, null, UUID.randomUUID().toString(), custm));
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                1, null, UUID.randomUUID().toString(), custm));

                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                2, null, UUID.randomUUID().toString(), custm));
            }

            //   sendTo remaining partitions
            for (long j = 0; j <remaining2Partitions /3; j++) {

                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());

                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                3, null, UUID.randomUUID().toString(), custm));
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                4, null, UUID.randomUUID().toString(), custm));
               /* KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                5, null, UUID.randomUUID().toString(), custm));

                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                6, null, UUID.randomUUID().toString(), custm));
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                7, null, UUID.randomUUID().toString(), custm));

                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                8, null, UUID.randomUUID().toString(), custm));*/

            }
            log.info("sent {} events Per Second ", Math.ceil(wrld.getDatay().get(i)));
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }

}
