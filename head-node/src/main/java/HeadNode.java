import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

public class HeadNode {


    private static int counter = 0;

    private static final Logger log = LoggerFactory.getLogger(HeadNode.class.getSimpleName());


    private static final UUID randomID = UUID.randomUUID();
    private static final String groupId = randomID.toString();
    private static String ipAddress = ConfigFileUpdater.getIpAddress() + ":9092";

    // this is for the meta data, maps image name to storage Node number
    private static HashMap<String, Integer> locationMap = new HashMap<>();

    private static HashMap<Integer, String> storageNodeNumberToTopicName = new HashMap<>();
    private static HashMap<String, Integer> topicNameToStorageNodeNumber = new HashMap<>();
    private static int storageNodeCount = 0;

    private static PriorityQueue<StorageNodeTuple> maxHeapStorageSpace = new PriorityQueue<>();

    /*
     * This is for the head node looking out for more storage nodes to join
     * SCALABILITY!!!
     **/
    static class StorageNodeLookout extends Thread {
        final int PORT = 12344;

        public StorageNodeLookout() {

        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(PORT)) {
                System.out.println("Server is listening on port " + PORT);

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client " + storageNodeCount + " connected: " + clientSocket.getInetAddress());

                    // Handle client communication in a new thread
                    Thread clientHandler = new Thread(() -> handleClient(clientSocket));
                    clientHandler.start();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private static void handleClient(Socket clientSocket) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream())) {
                // Read the Packet object from the client
                ProfilePacket packet = (ProfilePacket) objectInputStream.readObject();
                int currentStorageNodeCount = storageNodeCount;
                storageNodeCount++;

                // Print the received values
                System.out.println("Received topic name: " + packet.getTopicName());
                System.out.println("Received free space: " + packet.getFreeSpace());
                System.out.println("Received isSSD: " + packet.isSSD());

                if (topicNameToStorageNodeNumber.containsKey(packet.getTopicName())) {
                    try {
                        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
                        writer.println("Topic already exists! Choose another topic name.");

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return;
                } else {
                    try {
                        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);

                        writer.println("Topic does not exist! It will be created.");
                        createKafkaTopic(ipAddress + ":9092", packet.getTopicName());


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


                storageNodeNumberToTopicName.put(currentStorageNodeCount, packet.getTopicName());
                topicNameToStorageNodeNumber.put(packet.getTopicName(), currentStorageNodeCount);

                maxHeapStorageSpace.add(new StorageNodeTuple(packet.getFreeSpace(), packet.isSSD(), currentStorageNodeCount));


                System.out.println("storage node number to topic name: " + storageNodeNumberToTopicName);
                System.out.println("priority queue <space, number, isSSD>: " + maxHeapStorageSpace);


                while (true) {
                    System.out.println("Waiting for Hearbeat signal from storage node " + currentStorageNodeCount);
                    PeriodicHeartBeatPacket recvpacket = (PeriodicHeartBeatPacket) objectInputStream.readObject();
                    System.out.println("Received periodic heartbeat message from storage node " + currentStorageNodeCount + ": " + recvpacket.getMessage());
                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        public static void createKafkaTopic(String bootstrapServers, String topic) {
            System.out.println("Trying to create kafka topic");
            try {
                ProcessBuilder processBuilder = new ProcessBuilder(
                        "kafka-topics",
                        "--bootstrap-server", bootstrapServers,
                        "--topic", topic,
                        "--create"
                );

                // Redirect error stream to output stream
                processBuilder.redirectErrorStream(true);

                // Start the process
                Process process = processBuilder.start();

                // Wait for the process to finish
                int exitCode = process.waitFor();

                // Check the exit code
                if (exitCode == 0) {
                    System.out.println("Kafka topic created successfully.");
                } else {
                    System.err.println("Error creating Kafka topic. Exit code: " + exitCode);
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("Error in creating Kafka topic");
                e.printStackTrace();
            }
        }

    }

    /*
     * This is for the head node in constant lookout
     * for requests from Group-1
     * */
    static class ImageRequestLookout extends Thread {


        public ImageRequestLookout() {

        }

        @Override
        public void run() {

            /*
             * Images produced by the storage node
             * is being consumed and produced to
             * a topic consumed by Group-1
             * */
            Thread fetchImage = new Thread(() -> fetchImageHandler());
            fetchImage.start();

            String topicName = "request-topic";
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", ipAddress);
            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer", StringDeserializer.class.getName());
            properties.setProperty("group.id", groupId);
            properties.setProperty("auto.offset.reset", "latest");

            // create a consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

            // subscribe to the topic
            consumer.subscribe(Arrays.asList(topicName));

            try {
                // poll for data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Received request for image: " + record.value());

                        // requested image name
                        String reqImageName = record.value();


                        try {
                            // find which storage node has the image
                            String fileName = "metadata.json";
                            String keyToFind = reqImageName;


                            int value = findValueForKey(fileName, keyToFind);
                            if (value != Integer.MIN_VALUE) {
                                System.out.println("Value for key '" + keyToFind + "': " + value);
                            } else {
                                System.out.println("Error: Key '" + keyToFind + "' not found in the JSON file or not an integer.");
                                continue;
                            }

                            // send the request to the common topic "img-req-from-storage-node"
                            sendImageRequestToCommonTopic(keyToFind, value);

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


        // Function to find the value for a given key in a JSON file
        private static int findValueForKey(String fileName, String keyToFind) throws Exception {
            // Read the JSON file content into a string
            String content = new String(Files.readAllBytes(Paths.get(fileName)));

            // Parse the JSON string into a JSONObject
            JSONObject json = new JSONObject(content);

            // Check if the key exists in the JSON object
            if (json.has(keyToFind)) {
                // Retrieve the integer value associated with the key
                return json.getInt(keyToFind);
            } else {
                return Integer.MIN_VALUE; // Key not found or not an integer
            }
        }

        private static void fetchImageHandler() {
            // read the byte array from the common topic "img-from-storage-node"
            String topic = "img-from-storage-node";
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", ipAddress);
            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
            properties.setProperty("group.id", groupId);
            properties.setProperty("auto.offset.reset", "latest");

            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topic));

            try {
                // poll for data
                while (true) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        // send the image to the common topic "image-topic"
                        String topicToSendTo = "image-topic";

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
                        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicToSendTo, path, imageData);

                        // send the record (asynchronous) to storage
                        System.out.println("Sending byte array for image " + path + " to topic " + topicToSendTo);
                        producer.send(producerRecord);
                        producer.flush();
                        producer.close();
                    }


                }
            } catch (Exception e) {
                log.error("Unexpected exception in the head node", e);
            } finally {
                consumer.close();
                log.info("The head node is now gracefully shut down");
            }


        }

        private static void sendImageRequestToCommonTopic(String imgName, int storageNode) {
            String topic = "img-req-for-storage-node";

            // create Consumer Properties
            Properties properties = new Properties();

            // connect to Kafka broker(s)
            properties.setProperty("bootstrap.servers", ipAddress);
            properties.setProperty("group.id", groupId);
            properties.setProperty("auto.offset.reset", "latest");
            properties.setProperty("key.serializer", StringSerializer.class.getName());
            properties.setProperty("value.serializer", StringSerializer.class.getName());

            // create the producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            // create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, imgName, storageNodeNumberToTopicName.get(storageNode));

            System.out.println("Sending request for image " + imgName + " to topic " + topic);
            producer.send(producerRecord);
            producer.flush();
            producer.close();
        }


    }


    public static void main(String[] args) {
        log.info("I am the Head Node!");

        // ConfigFileUpdater.updateServerProperties();


        StorageNodeLookout storageNodeLookout = new StorageNodeLookout();
        storageNodeLookout.start();


        /*
        Thread threadPingAck = new Thread(new PingAck());
        threadPingAck.start();
        */

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
                    if (maxHeapStorageSpace.isEmpty()) {
                        System.out.println("How do you expect to have a distributed system with 0 Nodes???");
                        return;
                    }
                    StorageNodeTuple topTuple = maxHeapStorageSpace.poll();
                    String topicToSendTo = storageNodeNumberToTopicName.get(topTuple.getStorageNodeNumber());

                    log.info("Received message: Key - " + record.key());
                    log.info("Message routing to " + topicToSendTo);
                    log.info("Backup routing to ");

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
                    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicToSendTo, path, imageData);

                    // send the record (asynchronous) to storage
                    System.out.println("Sending image " + path + " to topic " + topicToSendTo);
                    producer.send(producerRecord);

                    // send the backup
                    if (maxHeapStorageSpace.isEmpty()) {
                        System.out.println("How do you expect to have a distributed system with 1 Node???");
                        return;
                    }

                    StorageNodeTuple topTupleForBackup = maxHeapStorageSpace.poll();
                    String topicToSendBackupTo = storageNodeNumberToTopicName.get(topTupleForBackup.getStorageNodeNumber());
                    producerRecord = new ProducerRecord<>(topicToSendBackupTo, path, imageData);
                    System.out.println("Sending backup image " + path + " to topic " + topicToSendBackupTo);
                    producer.send(producerRecord);

                    // update the priority queue
                    double updatedSpace = topTuple.getStorageSpace() - getSpaceInGB(imageData);
                    StorageNodeTuple updatedTuple = new StorageNodeTuple(topTuple.getStorageSpace(), topTuple.isSSD(), topTuple.getStorageNodeNumber());
                    maxHeapStorageSpace.add(updatedTuple);

                    updatedSpace = topTupleForBackup.getStorageSpace() - getSpaceInGB(imageData);
                    updatedTuple = new StorageNodeTuple(topTupleForBackup.getStorageSpace(), topTupleForBackup.isSSD(), topTupleForBackup.getStorageNodeNumber());
                    maxHeapStorageSpace.add(updatedTuple);


                    // updating the hashmap with the storage node the message got assigned to
                    // this is essentially the metadata
                    locationMap.put(record.key(), topTuple.getStorageNodeNumber());
                    locationMap.put("backup_" + record.key(), topTupleForBackup.getStorageNodeNumber());

                    // writing the meta data to a file
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
                        writer.writeValue(new File("/Users/ravisanker/Documents/Acads/Academics_4_1/DSTN/Project/meta/metadata.json"), locationMap);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (counter >= 0) {
                        String topicToSendMetaDataTo = "meta";
                        producerRecord = new ProducerRecord<>(topicToSendMetaDataTo, hashMapToByteArray(locationMap));
                        log.info("Metadata successfully sent to topic meta");
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


    }


    private static byte[] hashMapToByteArray(HashMap<String, Integer> hashMap) {
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

    private static double getSpaceInGB(byte[] imageData) {
        double sz = imageData.length;
        return sz / (1024.0 * 1024 * 1024);
    }
}

