import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class ImageRequester {
    public static void main(String[] args) {
        String filePath = "/Users/ravisanker/Documents/Acads/Academics_4_1/DSTN/Project/imgSent/img.txt";
        Path path = Paths.get(filePath);

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {

                System.out.println(line);
                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "10.70.47.171:9092");
                properties.setProperty("key.serializer", StringSerializer.class.getName());
                properties.setProperty("value.serializer", StringSerializer.class.getName());

                // create the producer
                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

                // create a Producer Record
                String topic = "request-topic";
                String key = new String(line);
                String value = new String(line);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line, value);

                producer.send(producerRecord);
                producer.flush();
                producer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
