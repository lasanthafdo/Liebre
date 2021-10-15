package common.tuple;

/**
 * This class needs to sit in Liebre for priority based stream support
 */
public class WatermarkedBaseRichTuple extends BaseRichTuple {

    public long endTimestamp;
    public String key;
    public String value;
    public long latency;
    public double throughput;

    protected final boolean watermark;

    public WatermarkedBaseRichTuple(long timestamp, String key, String value) {
        this(timestamp, key, value, false);
    }

    public WatermarkedBaseRichTuple(long timestamp, String key, String value, boolean watermark) {
        super(System.currentTimeMillis(), timestamp, String.valueOf(key));
        this.key = key;
        this.value = value;
        this.watermark = watermark;
    }

    public WatermarkedBaseRichTuple(long stimulus, long timestamp, String key, String value, boolean watermark) {
        super(stimulus, timestamp, String.valueOf(key));
        this.key = key;
        this.value = value;
        this.watermark = watermark;
    }

    public boolean isWatermark() {
        return watermark;
    }

    @Override
    public String toString() {
        return timestamp +
            ", " + endTimestamp +
            ", " + stimulus +
            ", " + key +
            ", " + value.replaceAll(",","|") +
            ", " + watermark +
            ", " + latency +
            ", " + String.format("%.3f", throughput);
    }
}
