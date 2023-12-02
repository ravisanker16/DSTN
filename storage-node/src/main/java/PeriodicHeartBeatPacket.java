import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class PeriodicHeartBeatPacket implements Serializable {
    private String message;
    private double memFreePerc;

    private double readSpeed;

    private double writeSpeed;
    private List<String> latestImagesReceieved;


    public PeriodicHeartBeatPacket(String message, List<String> latestImagesReceieved) {
        this.message = message;
        this.memFreePerc = 0;
        this.latestImagesReceieved = new ArrayList<>(latestImagesReceieved);
        this.readSpeed = 0;
        this.writeSpeed = 0;
    }

    public PeriodicHeartBeatPacket(String message) {
        this.message = message;
        this.memFreePerc = 0;
        this.latestImagesReceieved = null;
        this.readSpeed = 0;
        this.writeSpeed = 0;
    }

    public String getMessage() {
        return message;
    }

    public Double getMemFreePerc() {
        return memFreePerc;
    }

    public Double getReadSpeed() {
        return readSpeed;
    }

    public Double getWriteSpeed() {
        return writeSpeed;
    }

    public List<String> getLatestImagesList() {
        return latestImagesReceieved;
    }

    public void setLatestImages(List<String> stringList) {
        this.latestImagesReceieved = new ArrayList<>(stringList);
    }

    public void addString(String newString) {
        this.latestImagesReceieved.add(newString);
    }

    public void addProfile() {
        String os = System.getProperty("os.name").toLowerCase();
        String command;
        try {
            if (os.contains("mac")) {
                // System.out.println("Mac OS X is supported");
                command = "top -l 1 | awk '/PhysMem/ {printf(\"%.2f\\n\", $8/($8+$2*1000)*1000)}'";
            } else if (os.contains("nix") || os.contains("nux") || os.contains("nuxu")) {
                System.out.println("Linux is supported");

                command = "free -m | awk '/Mem:/ {printf(\"%.2f\\n\", ($4/$2)*100)}'";
            } else {
                System.out.println("Unsupported operating system");
                return;
            }

            Process process = new ProcessBuilder("bash", "-c", command).start();
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                InputStream is = process.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String line;

                while ((line = reader.readLine()) != null) {
                    System.out.println("Free memory percentage: " + line);
                    this.memFreePerc = Double.parseDouble(line);
                }

                reader.close();
            } else {
                // if command failed, set memory as not available
                System.out.println("Command execution failed with exit code: " + exitCode);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        /*
        String readCommand = "dd if=helper/wikimedia_file.txt of=/dev/null bs=4M";
        String writeCommand = "dd if=/dev/zero of=helper/wikimedia_file_write.txt bs=4M count=1000";

        try {
            System.out.println("Reading Speed:");
            Double rdSpeed = executeCommand(readCommand);
            if (rdSpeed != null) {
                this.readSpeed = rdSpeed;
                System.out.println("Read Speed: " + readSpeed + " GB/s");
            } else {
                System.err.println("Error extracting read speed.");
            }

            System.out.println("\nWriting Speed:");
            Double wrSpeed = executeCommand(writeCommand);
            if (wrSpeed != null) {
                this.writeSpeed = wrSpeed;
                System.out.println("Write Speed: " + writeSpeed + " GB/s");
            } else {
                System.err.println("Error extracting write speed.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private static Double executeCommand(String command) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(command.split("\\s+"));
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        Double GBPerSec = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            GBPerSec = extractSpeed(line);
            if (GBPerSec != null) {
                // Break the loop if a valid speed is obtained
                break;
            }
        }

        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("Command failed with exit code " + exitCode);
                return null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }

        return GBPerSec;
    }

    private static Double extractSpeed(String line) {
        Double GBPerSec = null;

        if (line.contains("bytes/sec")) {
            String[] parts = line.split("\\s+");
            String dataPerSec = parts[parts.length - 2].substring(1);
            Double bytesPerSecDouble = Double.parseDouble(dataPerSec);
            GBPerSec = bytesPerSecDouble / 1_000_000_000.0;
        } else if (line.contains("KB/s")) {
            // Handle KB/s case
        } else if (line.contains("MB/s")) {
            // Handle MB/s case
        } else if (line.contains("GB/s")) {
            String[] parts = line.split("\\s+");
            String dataPerSec = parts[parts.length - 2];
            Double GBPerSecDouble = Double.parseDouble(dataPerSec);
            GBPerSec = GBPerSecDouble;
        }

        return GBPerSec;

         */
    }
}
