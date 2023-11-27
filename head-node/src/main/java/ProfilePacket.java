import java.io.Serializable;

public class ProfilePacket implements Serializable {
    private String topicName;
    private double freeSpaceSSD;
    private double freeSpaceHDD;

    public ProfilePacket(String topicName, double freeSpaceSSD, double freeSpaceHDD) {
        this.topicName = topicName;
        this.freeSpaceSSD = freeSpaceSSD;
        this.freeSpaceHDD = freeSpaceHDD;
    }

    public String getTopicName() {
        return topicName;
    }

    public double getFreeSpaceSSD() {
        return freeSpaceSSD;
    }

    public double getFreeSpaceHDD() {
        return freeSpaceHDD;
    }
}
