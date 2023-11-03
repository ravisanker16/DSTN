/*
 * produce 3 images to the 'initial' topic
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class ImageProducer {
    private static final Logger log = LoggerFactory.getLogger(ImageProducer.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Image Produce");
        String imgName[] = new String[]{"blr.jpg", "maa.jpg", "tvm.jpg"};
        for (int i = 0; i < 3; i++) {
            String path = "/Users/ravisanker/Documents/Acads/Academics_4_1/DSTN/Project/img/" + imgName[i];
            File imageFile = new File(path);
            if (!imageFile.exists()) {
                System.err.println("Image file does not exist at the specified path: " + path);

            } else {
                System.out.println("File exits!");
            }
            // read image
            try {
                BufferedImage bImage = ImageIO.read(new File(path));
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ImageIO.write(bImage, "jpeg", bos);
                byte[] data = bos.toByteArray();

                // create producer properties
                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "10.50.1.3:9092");

                // set producer properties
                properties.setProperty("key.serializer", StringSerializer.class.getName());
                properties.setProperty("value.serializer", ByteArraySerializer.class.getName());

                // create the producer
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

                // create a Producer Record
                String topic = "initial";
                String key = imgName[i];
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
}
