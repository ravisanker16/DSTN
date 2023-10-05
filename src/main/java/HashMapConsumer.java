import com.fasterxml.jackson.databind.ObjectWriter;
<<<<<<< HEAD
=======
import com.fasterxml.jackson.databind.ObjectMapper;
>>>>>>> 55ad5c50687bf7b68a4bb73d1d78be3476da5cbb
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


public class HashMapConsumer {
    private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class.getSimpleName());

    public static <ObjectMapper> void main(String[] args) {
        log.info("I am a Kafka Hashmap consumer!");
        UUID random = UUID.randomUUID();

        String groupId = random.toString();
        groupId = "abc";
        String topic = "meta";

        // create Consumer Properties
        Properties properties = new Properties();

        // connect to Kafka broker(s)
        properties.setProperty("bootstrap.servers", "10.50.1.3:9092");

        // create consumer configs
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
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(10000));
                log.info("outside for");
                for (ConsumerRecord<String, byte[]> record : records) {
                    log.info("Received message: Key - " + record.key());

                    // Assuming the value is an image byte array (JPEG format)
                    byte[] value = record.value();
                    log.info("before tryyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy");
                    try (ByteArrayInputStream bis = new ByteArrayInputStream(value);
                         ObjectInputStream ois = new ObjectInputStream(bis)) {
                        log.info("insideeeeeeeeeeeeeeeeeeeeeee");
<<<<<<< HEAD
                        HashMap<String, Integer> deserializedUser = (HashMap<String, Integer>) ois.readObject();
=======
                        HashMap<String, Integer> deserializedUser = (HashMap<String,Integer>)ois.readObject();
>>>>>>> 55ad5c50687bf7b68a4bb73d1d78be3476da5cbb
                        System.out.println(deserializedUser);
                        ObjectMapper mapper = new ObjectMapper();
                        try {
                            ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
                            writer.writeValue(new File("/home/rahul/Documents/DSTN-main/DSTN-main/meta/metadata.json"), deserializedUser);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
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
