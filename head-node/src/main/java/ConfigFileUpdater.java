import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;


public class ConfigFileUpdater {
    public static void updateServerProperties() {
        String configFile = "/usr/local/etc/kafka/server.properties"; // Specify the path to your Kafka configuration file
        String newIpAddress = getIpAddress(); // Implement a method to get the new IP address

        try {
            // Read the content of the original configuration file
            BufferedReader reader = new BufferedReader(new FileReader(configFile));
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(System.lineSeparator());
            }
            reader.close();

            // Replace the placeholder with the new IP address
            String updatedContent = content.toString().replace("%BROKER_IP%", newIpAddress);

            // Write the updated content back to the configuration file
            FileWriter writer = new FileWriter(configFile);
            writer.write(updatedContent);
            writer.close();

            System.out.println("Configuration file updated successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getIpAddress() {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("google.com", 80));
            System.out.println("My IP is " + socket.getLocalAddress());
            return socket.getLocalAddress().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "127.0.0.1";
        } catch (IOException e) {
            e.printStackTrace(); // Handle the IOException
            return "127.0.0.1";
        }
    }

}
