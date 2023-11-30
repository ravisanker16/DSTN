import java.io.Serializable;

public class ProfilePacket implements Serializable {
    private String topicName;
    private double freeSpace;
    private boolean isSSD;

    public ProfilePacket(String topicName, double freeSpace, boolean isSSD) {
        this.topicName = topicName;
        this.freeSpace = freeSpace;
        this.isSSD = isSSD;
    }

    public String getTopicName() {
        return topicName;
    }

    public double getFreeSpace() {
        return freeSpace;
    }

    public boolean isSSD() {
        return isSSD;
    }
}
