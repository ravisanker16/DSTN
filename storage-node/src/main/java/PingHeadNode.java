import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

class PingHeadNode implements Runnable {
    private String groupId;
    private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class.getSimpleName());
    private static final String ipAddress = "10.50.7.242:9092";

    public void run() {
        String topic = "ping";
        String value = "ACK";
        Properties properties = new Properties();

        UUID random = UUID.randomUUID();
        groupId = random.toString();

        properties.setProperty("bootstrap.servers", ipAddress);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        Timer timer = new Timer();
        long delay = 0; // Initial delay in milliseconds
        long period = 5000; // 5 seconds in milliseconds

        timer.scheduleAtFixedRate(new TimerTask() {
            int cntPing = 0;

            public void run() {
                if (cntPing != 0) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    if (records.isEmpty()) {
                        log.info("HEAD NODE DOWN! I REPEAT, HEAD NODE DOWN!!!");
                        String topicRed = "red";
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicRed, "Red");
                        producer.send(producerRecord);
                        producer.flush();
                    }
                }
                String key = Integer.toString(cntPing);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                producer.send(producerRecord);
                producer.flush();
                cntPing++;


            }
        }, delay, period);

        // close the producer
        producer.close();
    }
}
