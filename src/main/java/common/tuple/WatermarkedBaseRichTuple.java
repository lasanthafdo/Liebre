package common.tuple;

public class WatermarkedBaseRichTuple extends BaseRichTuple {

    public long endTimestamp;
    public int key;
    public int value;
    public long latency;
    public double throughput;

    private final long timestamp;
    private final boolean watermark;

    public WatermarkedBaseRichTuple(long timestamp, int key, int value) {
        this(timestamp, key, value, false);
    }

    public WatermarkedBaseRichTuple(long timestamp, int key, int value, boolean watermark) {
        super(System.currentTimeMillis(), timestamp, String.valueOf(key));
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.watermark = watermark;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String getKey() {
        return String.valueOf(key);
    }

    public boolean isWatermark() {
        return watermark;
    }

    @Override
    public String toString() {
        return timestamp +
            ", " + endTimestamp +
            ", " + key +
            ", " + value +
            ", " + watermark +
            ", " + latency +
            ", " + String.format("%.3f", throughput);
    }
}
