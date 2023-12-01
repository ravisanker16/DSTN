//import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class ImageConsumer {

    private static final String groupId = "saketmac";
    private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class.getSimpleName());
    private static final String ipAddress = "10.70.47.171:9092";
    private static final long ONE_MINUTE = 60 * 1000;

    private static class PeriodicHeartBeatSender extends Thread {
        private final Socket socket;
        private final ObjectOutputStream objectOutputStream;

        public PeriodicHeartBeatSender(Socket socket, ObjectOutputStream objectOutputStream) {
            this.socket = socket;
            this.objectOutputStream = objectOutputStream;
        }

        @Override
        public void run() {

            Timer timer = new Timer(true);

            // Schedule a task to run every 1 minute
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        // Create a new Packet object for periodic sending
                        PeriodicHeartBeatPacket periodicPacket = new PeriodicHeartBeatPacket("alive");
                        // get current profile
                        periodicPacket.addProfile();
                        // Send the periodic Packet to the server
                        objectOutputStream.writeObject(periodicPacket);

                        System.out.println("Periodic packet sent to server.");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }, 0, ONE_MINUTE);
        }
    }

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        double myFreeSpaceSSD = StorageCapacity.getFreeSpaceSSD();
        double myFreeSpaceHDD = StorageCapacity.getFreeSpaceHDD();
        double myFreeSpace = myFreeSpaceSSD != 0 ? myFreeSpaceSSD : myFreeSpaceHDD;
        boolean isSSD = myFreeSpaceSSD != 0 ? true : false;

        String topicName = "storagenode_1";

        final String SERVER_HOST = "10.70.47.171";
        final int SERVER_PORT = 12344;

        Socket socket = null;
        ObjectOutputStream objectOutputStream = null;

        try {
            // Create a Socket outside the try-with-resources block
            socket = new Socket(SERVER_HOST, SERVER_PORT);

            // Create an ObjectOutputStream outside the try-with-resources block
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

            // Create a Packet object to send
            ProfilePacket packetToSend = new ProfilePacket(topicName, myFreeSpace, isSSD);

            // Send the Packet to the server
            objectOutputStream.writeObject(packetToSend);

            System.out.println("Packet sent to server.");

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String message = reader.readLine();
            System.out.println("Received message from head node: " + message);

            // Start a new thread for periodic message sending
            new PeriodicHeartBeatSender(socket, objectOutputStream).start();

            System.out.println("Periodic Heard Beat Sender thread started");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close resources when they are no longer needed

        }



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
        consumer.subscribe(Arrays.asList(topicName));

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

                        path = "/Users/saket/Desktop/BITS_4-1/DSTN/Project/Codes/DSTN/rcv/";
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


    }
}
