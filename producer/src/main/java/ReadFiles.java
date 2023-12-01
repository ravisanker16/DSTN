import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ReadFiles {
    // List to store file names
    private static List<String> fileNamesList = new ArrayList<>();

    // Getter for file names list
    public static List<String> getFileNamesList() {
        return fileNamesList;
    }

    public static void printFileNames(File[] a, int i, int lvl) {
        if (i == a.length) {
            return;
        }
        if (a[i].isFile() && a[i].getName() != ".DS_Store") {
            // Instead of printing, add file name to the list
            fileNamesList.add(a[i].getName());
        }
        printFileNames(a, i + 1, lvl);
    }

    // Main Method
    public static void addFileNames() {
        String path = "/Users/ravisanker/Documents/Acads/Academics_4_1/DSTN/Project/img";
        File fObj = new File(path);
        ReadFiles obj = new ReadFiles();
        if (fObj.exists() && fObj.isDirectory()) {
            File a[] = fObj.listFiles();

            obj.printFileNames(a, 0, 0);

            // Accessing the file names list using getters
            List<String> fileNames = obj.getFileNamesList();

            // Displaying the stored file names
            System.out.println("Stored File Names:");
            for (String fileName : fileNames) {
                System.out.println(fileName);
            }
        }
    }
}
