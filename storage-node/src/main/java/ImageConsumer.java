//import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ImageConsumer {
    private static final String ipAddress = "10.70.47.171:9092";

    private static final UUID randomID = UUID.randomUUID();
    private static final String groupId = randomID.toString();
    private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class.getSimpleName());
    private static final long ONE_MINUTE = 60 * 1000;
    private static String topicName = "storagenode_1";
    private static String topicToConsumeRequests = "img-req-for-storage-node";
    private static List<String> latestImagesReceived = new ArrayList<String>();

    /*
     * This is for sending periodic heart beats
     * to the head node
     * telling it you're alive
     *
     * */
    private static class PeriodicHeartBeatSender extends Thread {
        private final Socket socket;
        private final ObjectOutputStream objectOutputStream;
        private final ObjectInputStream objectInputStream;

        public PeriodicHeartBeatSender(Socket socket, ObjectOutputStream objectOutputStream, ObjectInputStream objectInputStream) {
            this.socket = socket;
            this.objectOutputStream = objectOutputStream;
            this.objectInputStream = objectInputStream;
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
                        PeriodicHeartBeatPacket periodicPacket = new PeriodicHeartBeatPacket("alive", latestImagesReceived);
                        periodicPacket.addProfile();
                        objectOutputStream.writeObject(periodicPacket);
                        latestImagesReceived.clear();
                        System.out.println("Periodic packet sent to server.");
                        System.out.println("Waiting for ack from head node");

                        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

                        // Schedule a task to be executed after 90 seconds
                        executorService.schedule(() -> {
                            // This code will be executed after the timeout (30 seconds)
                            System.out.println("No ack received within the timeout. Taking appropriate actions.");

                            executorService.shutdown();
                        }, 90, TimeUnit.SECONDS);

                        PeriodicHeartBeatPacket recvPack;
                        try {
                            recvPack = (PeriodicHeartBeatPacket) objectInputStream.readObject();
                            // If an ack is received before the timeout, cancel the scheduled task
                            executorService.shutdownNow();
                            System.out.println("Received: " + recvPack.getMessage());
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        } finally {
                            // Make sure to shutdown the executor service in case of any exception
                            executorService.shutdown();
                        }

                    } catch (IOException e) {
                        /*
                         * handle head node going down here
                         * */
                        

                        e.printStackTrace();
                    }
                }
            }, 0, ONE_MINUTE);
        }
    }

    /*
     * This is for handling image requests
     * sent by head node
     * and sending the image if it has
     * to the common topic for head node to read
     *
     **/
    private static void imageRequestHandler() {
        // key->image name, value->topic name
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ipAddress);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicToConsumeRequests));

        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received image request for: " + record.key());

                    // Assuming the value ia the topic name to which this storage node is subscribed to.
                    String rcvTopicName = record.value();
                    if (!rcvTopicName.equals(topicName))
                        continue;

                    // Process and save the image to a file
                    try {
                        String imgName = record.key();
                        String imgPath = "/Users/saket/Desktop/BITS_4-1/DSTN/Project/Codes/DSTN/rcv/" + imgName;
                        byte[] fileBytes = readBytesFromFile(imgPath);
                        sendImageToCommonTopic(imgName, fileBytes);
                    } catch (IOException e) {
                        System.out.println("Error while processing or saving the image");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Unexpected exception in the consumer");
        } finally {
            consumer.close(); // close the consumer
            System.out.println("The consumer is now gracefully shut down");
        }
    }

    private static void sendImageToCommonTopic(String imgName, byte[] fileBytes) {
        String topic = "img-from-storage-node";

        // create Consumer Properties
        Properties properties = new Properties();

        // connect to Kafka broker(s)
        properties.setProperty("bootstrap.servers", ipAddress);
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", ByteArraySerializer.class.getName());

        // create the producer
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, imgName, fileBytes);

        System.out.println("Sending image " + imgName + " to topic " + topic);
        producer.send(producerRecord);
        producer.flush();
        producer.close();
    }

    private static byte[] readBytesFromFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return Files.readAllBytes(path);
    }

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");


        Thread imageRequestThread = new Thread(() -> imageRequestHandler());
        imageRequestThread.start();

        double myFreeSpaceSSD = StorageCapacity.getFreeSpaceSSD();
        double myFreeSpaceHDD = StorageCapacity.getFreeSpaceHDD();
        double myFreeSpace = myFreeSpaceSSD != 0 ? myFreeSpaceSSD : myFreeSpaceHDD;
        boolean isSSD = myFreeSpaceSSD != 0 ? true : false;


        final String SERVER_HOST = "10.70.47.171";
        final int SERVER_PORT = 12344;

        Socket socket = null;
        ObjectOutputStream objectOutputStream = null;
        ObjectInputStream objectInputStream = null;

        try {

            socket = new Socket(SERVER_HOST, SERVER_PORT);
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectInputStream = new ObjectInputStream(socket.getInputStream());

            ProfilePacket packetToSend = new ProfilePacket(topicName, myFreeSpace, isSSD);
            objectOutputStream.writeObject(packetToSend);

            System.out.println("Packet sent to server.");

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String message = reader.readLine();
            System.out.println("Received message from head node: " + message);
            new PeriodicHeartBeatSender(socket, objectOutputStream, objectInputStream).start();

            System.out.println("Periodic Heard Beat Sender thread started");

        } catch (IOException e) {
            System.out.println("khfdskjfhsjkfh");
            e.printStackTrace();
        } finally {

        }

        /*
        This is the receiving images from Group-1 part
        Consume from my assigned topic and store it locally
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
                        String imagePath = "/Users/saket/Desktop/BITS_4-1/DSTN/Project/Codes/DSTN/rcv/" + record.key();
                        FileUtils.writeByteArrayToFile(new File(imagePath), imageData);
                        log.info("Saved image to: " + imagePath);
                        latestImagesReceived.add(record.key());

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
