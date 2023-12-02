class StorageNodeTuple implements Comparable<StorageNodeTuple> {
    /*
        w1 * x1 + w2 * x2
        w1 = wtSpace
        x1 = storage space
        w2 = weight of bytes sent
        x2 = number of bytes actually sent
     */


    private double wtStorageSpace; // function of isSSD
    private double wtBytesSent;

    private double wtReadWrite;
    private double wtmemFreePerc;
    private double storageSpace;
    private double bytesSent;

    private double memFreePerc;
    private double readWrite;
    private boolean isSSD;
    private int storageNodeNumber;

    private double totWeightNode;

    private final int ratio = 2;

    public StorageNodeTuple(double storageSpace, double bytesSent, double memFreePerc, double readWrite, boolean isSSD, int storageNodeNumber) {
        this.storageSpace = storageSpace;
        this.bytesSent = bytesSent;
        this.memFreePerc = memFreePerc;
        this.readWrite = readWrite;
        this.isSSD = isSSD;
        this.storageNodeNumber = storageNodeNumber;
        this.wtStorageSpace = isSSD ? 0.5 : 0.3;
        this.wtBytesSent = -2 * (1 / (1024 * 1024 * 1024)); // CHANGE!!!
        this.wtmemFreePerc = 0.2;
        this.wtReadWrite = 0.2;
        setTotalWeight();
    }


    // Implement compareTo method for Comparable interface
    @Override
    public int compareTo(StorageNodeTuple other) {
        return Double.compare(other.totWeightNode, totWeightNode); // Reverse order for max heap
    }

    // Add getters if needed


    public double getStorageSpace() {
        return storageSpace;
    }

    public int getStorageNodeNumber() {
        return storageNodeNumber;
    }

    public double getBytesSent(){ return bytesSent; }

    public double getMemFreePerc(){ return memFreePerc; }

    public double getReadWrite(){ return readWrite; }


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
        this.totWeightNode = wtStorageSpace * storageSpace + wtBytesSent * bytesSent + wtmemFreePerc * memFreePerc
        + wtReadWrite * readWrite;
    }


    public String toString() {
        return "(wt=" + totWeightNode + ", node=" + storageNodeNumber + ",freeSpace=" + storageSpace + ")";
    }
}
