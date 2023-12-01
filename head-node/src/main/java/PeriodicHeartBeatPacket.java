import java.io.*;

public class PeriodicHeartBeatPacket implements Serializable {
    private String message;
    private double memFreePerc;


    public PeriodicHeartBeatPacket(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void addProfile() {
        String os = System.getProperty("os.name").toLowerCase();
        // System.out.println("Operating system: " + os);
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
                memFreePerc = 0.0;
                System.out.println("Command execution failed with exit code: " + exitCode);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


}
