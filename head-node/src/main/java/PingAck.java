import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class PingAck implements Runnable {
    private static String groupId;
    private static final Logger log = LoggerFactory.getLogger(PingAck.class.getSimpleName());
    private static final String ipAddress = "10.70.33.130:9092";

    public void run() {
        String topicFrom = "ping";
        String topicTo = "pingACK";

        UUID random = UUID.randomUUID();
        groupId = random.toString();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ipAddress);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to the topic
        consumer.subscribe(Arrays.asList(topicFrom));

        try {
            // poll for data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received message: Key - " + record.key() + " Value - " + record.value());

                    properties.setProperty("bootstrap.servers", ipAddress);
                    properties.setProperty("key.serializer", StringSerializer.class.getName());
                    properties.setProperty("value.serializer", StringSerializer.class.getName());

                    // create the producer
                    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

                    // create a Producer Record
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicTo, record.key(), record.value());
                    producer.send(producerRecord);


                    producer.flush();

                    // close the producer
                    producer.close();
                }


            }
        } catch (Exception e) {
            log.error("Unexpected exception in the head node", e);
        } finally {
            consumer.close();
            log.info("The pingACK thread is now gracefully shut down");
        }


    }

}
