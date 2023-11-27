import java.io.File;

public class StorageCapacity {
    public static double getFreeSpaceSSD() {
        // Get the root directory of the file system
        File rootDirectory = new File("/");
        double freeGB = 0;
        double totalGB = 0;
        // Check if the root directory exists
        if (rootDirectory.exists()) {
            // Retrieve the total space (capacity) and free space (available space)
            long totalSpace = rootDirectory.getTotalSpace();
            long freeSpace = rootDirectory.getFreeSpace();

            // Convert the bytes to more human-readable units (e.g., gigabytes)
            totalGB = totalSpace / (1024.0 * 1024 * 1024);
            freeGB = freeSpace / (1024.0 * 1024 * 1024);
            
            System.out.println("Available storage space: " + freeGB + " GB (in SSD)");
        } else {
            System.err.println("Root directory does not exist. Unable to determine storage capacity.");
        }

        return freeGB;
    }

    public static double getFreeSpaceHDD() {
        // If you do have a HDD, copy paste the code for getFreeSpaceSSD
        // and change the rootDirectory
        return 0;
    }
}
