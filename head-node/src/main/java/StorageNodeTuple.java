class StorageNodeTuple implements Comparable<StorageNodeTuple> {
    /*
        w1 * x1 + w2 * x2
        w1 = wtSpace
        x1 = storage space
        w2 = weight of bytes sent
        x2 = number of bytes actually sent
     */


    private double wtSpace; // function of isSSD
    private double wtBytesSent;

    private double storageSpace;
    private double bytesSent;
    private boolean isSSD;
    private int storageNodeNumber;

    private double totWeightNode;

    private final int ratio = 2;

    public StorageNodeTuple(double storageSpace, boolean isSSD, int storageNodeNumber) {
        this.storageSpace = storageSpace;
        this.isSSD = isSSD;
        this.storageNodeNumber = storageNodeNumber;
        this.bytesSent = 0;
        this.wtSpace = isSSD ? 0.5 : 0.3;
        this.wtBytesSent = -2 * (1 / (1024 * 1024 * 1024));
        setTotalWeight();
    }

    // Implement compareTo method for Comparable interface
    @Override
    public int compareTo(StorageNodeTuple other) {
        return Double.compare(totWeightNode, other.totWeightNode); // Reverse order for max heap
    }

    // Add getters if needed


    public double getStorageSpace() {
        return storageSpace;
    }

    public int getStorageNodeNumber() {
        return storageNodeNumber;
    }

    public boolean isSSD() {
        return isSSD;
    }

    public void setStorageSpace(double storageSpace) {
        this.storageSpace = storageSpace;
    }

    public void setBytesSent(double bytesSent) {
        this.bytesSent = bytesSent;
    }

    public void setTotalWeight() {
        this.totWeightNode = wtSpace * storageSpace + wtBytesSent * bytesSent;
    }


    public String toString() {
        return "(wt=" + totWeightNode + ", node=" + storageNodeNumber + ",freeSpace=" + storageSpace + ")";
    }
}
