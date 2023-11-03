import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;


public class HashMapConsumer implements Runnable {

    private String groupId;
    private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class.getSimpleName());
    private static final String ipAddress = "10.50.7.242:9092";

    public void run() {
        log.info("I am a Kafka Meta Data Consumer!");

        UUID random = UUID.randomUUID();
        groupId = random.toString();

        String topic = "meta";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ipAddress);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");

        // create a consumer
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);

        // subscribe to the topic
        consumer.subscribe(Arrays.asList(topic));

        try {
            // poll for data
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    log.info("Received message: Key - " + record.key());

                    // Assuming the value is an image byte array (JPEG format)
                    byte[] value = record.value();
                    HashMap<String, Integer> deserializedUser;
                    try (ByteArrayInputStream bis = new ByteArrayInputStream(value); ObjectInputStream ois = new ObjectInputStream(bis)) {
                        deserializedUser = (HashMap<String, Integer>) ois.readObject();
                        System.out.println(deserializedUser);
                    }

                    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                    try {
                        com.fasterxml.jackson.databind.ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
                        writer.writeValue(new File("/home/rahul/Documents/DSTN-main/DSTN-main/meta/metadata.json"), deserializedUser);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    // Process and save the image to a file

                }
            }
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer
            log.info("The consumer is now gracefully shut down");
        }
    }
}
