import java.io.Serializable;

public class PeriodicHeartBeatPacket implements Serializable {
    private String message;

    public PeriodicHeartBeatPacket(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }


}
