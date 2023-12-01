import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class ImageProducer {
    private static final Logger log = LoggerFactory.getLogger(ImageProducer.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Image Produce");
        ReadFiles.addFileNames(10);
        List<String> imgName = ReadFiles.getFileNamesList();
        writeListToFile(imgName, "/Users/ravisanker/Documents/Acads/Academics_4_1/DSTN/Project/imgSent/");

        for (int i = 0; i < imgName.size(); i++) {
            String path = "/Users/ravisanker/Desktop/row_wise/" + imgName.get(i);
            File imageFile = new File(path);
            if (!imageFile.exists()) {
                System.err.println("Image file does not exist at the specified path: " + path);

            } else {
                System.out.println("File exits!");
            }
            // read image
            try {

                byte[] data = readFileToByteArray(path);

                // create producer properties
                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "10.70.47.171:9092");

                // set producer properties
                properties.setProperty("key.serializer", StringSerializer.class.getName());
                properties.setProperty("value.serializer", ByteArraySerializer.class.getName());

                // create the producer
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

                // create a Producer Record
                String topic = "initial";
                String key = imgName.get(i);
                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, key, data);

                // send the record (asynchronous)
                producer.send(producerRecord);

                // tell the producer to send all data and block until done (synchronous call)
                producer.flush();

                // close the producer
                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void writeListToFile(List<String> lines, String filePath) {
        Path path = Paths.get(filePath);

        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            // Write each line from the list to the file
            for (String line : lines) {
                writer.write(line);
                writer.newLine(); // Add a newline after each line
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] readFileToByteArray(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return Files.readAllBytes(path);
    }
}
