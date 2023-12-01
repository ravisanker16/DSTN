import java.io.Serializable;

public class ProfilePacket implements Serializable {
    private String topicName;
    private double freeSpace;
    private boolean isSSD;
    private boolean firstTime;

    public ProfilePacket(String topicName, double freeSpace, boolean isSSD, boolean firstTime) {
        this.topicName = topicName;
        this.freeSpace = freeSpace;
        this.isSSD = isSSD;
        this.firstTime = firstTime;
    }

    public ProfilePacket(String topicName, double freeSpace, boolean isSSD) {
        this.topicName = topicName;
        this.freeSpace = freeSpace;
        this.isSSD = isSSD;
        this.firstTime = true;
    }

    public String getTopicName() {
        return topicName;
    }

    public boolean getFirstTime() {
        return firstTime;
    }

    public double getFreeSpace() {
        return freeSpace;
    }

    public boolean isSSD() {
        return isSSD;
    }
}
