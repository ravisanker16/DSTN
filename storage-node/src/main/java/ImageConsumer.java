import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ImageConsumer {

    private static String groupId;
    private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class.getSimpleName());
    private static final String ipAddress = "10.70.33.130:9092";

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        double myFreeSpaceSSD = StorageCapacity.getFreeSpaceSSD();
        double myFreeSpaceHDD = StorageCapacity.getFreeSpaceHDD();
        double myFreeSpace = myFreeSpaceSSD != 0 ? myFreeSpaceSSD : myFreeSpaceHDD;
        boolean isSSD = myFreeSpaceSSD != 0 ? true : false;

        String topicName = "storage_0";

        final String SERVER_HOST = "localhost";
        final int SERVER_PORT = 12345;

        try (Socket socket = new Socket(SERVER_HOST, SERVER_PORT);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream())) {

            // Create a Packet object to send
            ProfilePacket packetToSend = new ProfilePacket(topicName, myFreeSpace, isSSD);

            // Send the Packet to the server
            objectOutputStream.writeObject(packetToSend);

            System.out.println("Packet sent to server.");

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String message = reader.readLine();
            System.out.println("Received message from head node: " + message);

        } catch (IOException e) {
            e.printStackTrace();
        }


//        UUID random = UUID.randomUUID();
//        groupId = random.toString();

        /*
        Thread threadPing = new Thread(new PingHeadNode());
        threadPing.start();


        /*
         * Uncomment this only if this Storage Node is acting as the Backup Head Node
        Thread threadMeta = new Thread(new HashMapConsumer());
        threadMeta.start();
        Thread threadBackup = new Thread(new ImageConsumerBackup());
        threadBackup.start();
        */

        /*
        String topic = "storage0";

        // create Consumer Properties
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
                    byte[] imageData = record.value();

                    // Process and save the image to a file
                    try {
                        BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageData));
                        String path = record.key().substring(0, record.key().length() - 4);
                        String token[] = path.split("/");

                        path = "/Users/ravisanker/Documents/Acads/Academics_4_1/DSTN/Project/img/";
                        String imagePath;

                        imagePath = path + token[token.length - 1] + "_recv.jpg";

                        File outputFile = new File(imagePath);
                        outputFile.createNewFile();

                        ImageIO.write(image, "jpg", outputFile);
                        log.info("Saved image to: " + imagePath);
                    } catch (IOException e) {
                        log.error("Error while processing or saving the image", e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer
            log.info("The consumer is now gracefully shut down");
        }
        */


    }
}
