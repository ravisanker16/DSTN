import java.io.File;

public class StorageCapacity {
    public static void main(String[] args) {
        // Get the root directory of the file system
        File rootDirectory = new File("/");

        // Check if the root directory exists
        if (rootDirectory.exists()) {
            // Retrieve the total space (capacity) and free space (available space)
            long totalSpace = rootDirectory.getTotalSpace();
            long freeSpace = rootDirectory.getFreeSpace();

            // Convert the bytes to more human-readable units (e.g., gigabytes)
            double totalGB = totalSpace / (1024.0 * 1024 * 1024);
            double freeGB = freeSpace / (1024.0 * 1024 * 1024);

            System.out.println("Total storage capacity: " + totalGB + " GB");
            System.out.println("Available storage space: " + freeGB + " GB");
        } else {
            System.err.println("Root directory does not exist. Unable to determine storage capacity.");
        }
    }
}
