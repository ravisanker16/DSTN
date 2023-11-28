class StorageNodeTuple implements Comparable<StorageNodeTuple> {
    private double storageSpace;
    private int isSSD;
    private int storageNodeNumber;

    private final int ratio = 2;

    public StorageNodeTuple(double storageSpace, int isSSD, int storageNodeNumber) {
        this.storageSpace = storageSpace;
        this.isSSD = isSSD;
        this.storageNodeNumber = storageNodeNumber;
    }

    // Implement compareTo method for Comparable interface
    @Override
    public int compareTo(StorageNodeTuple other) {
        if (other.storageNodeNumber == storageNodeNumber) {
            if (isSSD == 1)
                return -1;
            return 1;
        }

        double node1Value = storageSpace;
        double node2Value = other.storageSpace;

        if (isSSD == 1)
            node1Value *= ratio;

        if (other.isSSD == 1)
            node2Value *= ratio;

        return Double.compare(node1Value, node2Value); // Reverse order for max heap
    }

    // Add getters if needed


    public double getStorageSpace() {
        return storageSpace;
    }

    public int getStorageNodeNumber() {
        return storageNodeNumber;
    }
    

    public String toString() {
        return "(" + storageSpace + ", " + storageNodeNumber + ", " + isSSD + ")";
    }
}
