import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class HeadNode {

    private static final String topicName[] = new String[]{
            "storage0",
            "storage1",
            "storage2",
            "storage0_backup",
            "storage1_backup",
            "storage2_backup"
    };

    private static int counter = 0;

    private static final Logger log = LoggerFactory.getLogger(HeadNode.class.getSimpleName());

    private static HashMap<String, Integer> locationMap = new HashMap<>();

    private static String groupId = "i-am-head-node";
    private static String ipAddress = "10.70.33.130:9092";

    public static byte[] hashMapToByteArray(HashMap<String, Integer> hashMap) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            // Serialize the HashMap to the ObjectOutputStream
            objectOutputStream.writeObject(hashMap);

            // Close the streams
            objectOutputStream.close();
            byteArrayOutputStream.close();

            // Get the byte array from the ByteArrayOutputStream
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception as needed
        }

        return null; // Return null if there was an error
    }

    public static void main(String[] args) {
        log.info("I am the Head Node!");

        final int PORT = 12345;

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server is listening on port " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress());

                // Handle client communication in a new thread
                Thread clientHandler = new Thread(() -> handleClient(clientSocket));
                clientHandler.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        /*
        Thread threadPingAck = new Thread(new PingAck());
        threadPingAck.start();

        String topic = "initial";

        // create Consumer Properties
        Properties properties = new Properties();

        // connect to Kafka broker(s)
        properties.setProperty("bootstrap.servers", ipAddress);

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
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, byte[]> record : records) {
                    log.info("Received message: Key - " + record.key());
                    log.info("Message routing to " + topicName[counter % 3]);
                    log.info("Backup routing to " + topicName[(counter % 3) + 3]);

                    // Assuming the value is an image byte array (JPEG format)
                    byte[] imageData = record.value();
                    String path = record.key();

                    // Send this data to the appropriate topic
                    properties.setProperty("bootstrap.servers", ipAddress);

                    // set producer properties
                    properties.setProperty("key.serializer", StringSerializer.class.getName());
                    properties.setProperty("value.serializer", ByteArraySerializer.class.getName());

                    // create the producer
                    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

                    // create a Producer Record
                    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicName[counter % 3], path, imageData);

                    // send the record (asynchronous) to storage
                    producer.send(producerRecord);

                    // send the topic to the topic
                    producerRecord = new ProducerRecord<>(topicName[(counter % 3) + 3], path, imageData);
                    producer.send(producerRecord);


                    // updating the hashmap with the storage node the message got assigned to
                    // this is essentially the metadata
                    locationMap.put(record.key(), counter % 3);
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
                        writer.writeValue(new File("/Users/ravisanker/Documents/Acads/Academics_4_1/DSTN/Project/meta/metadata.json"), locationMap);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (counter >= 0) {
                        producerRecord = new ProducerRecord<>("meta", hashMapToByteArray(locationMap));
                        log.info("Meta data successfully sent to topic meta");
                        producer.send(producerRecord);
                    }

                    counter++;

                    // tell the producer to send all data and block until done (synchronous call)
                    producer.flush();

                    // close the producer
                    producer.close();
                }


            }
        } catch (Exception e) {
            log.error("Unexpected exception in the head node", e);
        } finally {
            consumer.close();
            log.info("The head node is now gracefully shut down");
        }
        */


    }

    private static void handleClient(Socket clientSocket) {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream())) {
            // Read the Packet object from the client
            ProfilePacket packet = (ProfilePacket) objectInputStream.readObject();

            // Print the received values
            System.out.println("Received topic name: " + packet.getTopicName());
            System.out.println("Received free space in SSD: " + packet.getFreeSpaceSSD());
            System.out.println("Received free space in HDD: " + packet.getFreeSpaceHDD());

            System.out.println("Client disconnected: " + clientSocket.getInetAddress());

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
