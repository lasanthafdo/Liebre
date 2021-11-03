package common.tuple;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class needs to sit in Liebre for priority based stream support
 */
public class WatermarkedBaseRichTuple extends BaseRichTuple {

    public long endTimestamp;
    public String key;
    public String value;
    public long latency;
    public double throughput;
    private BlockingQueue<ImmutablePair<String, Long>> timestampMap;

    protected final boolean watermark;

    public WatermarkedBaseRichTuple(long timestamp, String key, String value) {
        this(timestamp, key, value, false);
    }

    public WatermarkedBaseRichTuple(long timestamp, String key, String value, boolean watermark) {
        this(System.currentTimeMillis(), timestamp, key, value, watermark);
    }

    public WatermarkedBaseRichTuple(long stimulus, long timestamp, String key, String value, boolean watermark) {
        super(stimulus, timestamp, String.valueOf(key));
        this.key = key;
        this.value = value;
        this.watermark = watermark;
        if(watermark) {
            this.timestampMap = new LinkedBlockingQueue<>();
        }
    }

    public boolean addTimestampMarker(String operatorId, Long timestamp) {
        if(this.watermark) {
            if(this.timestampMap.stream().anyMatch(tsMarker -> tsMarker.getKey().equals(operatorId))) {
                return false;
            } else {
                this.timestampMap.add(new ImmutablePair<>(operatorId, timestamp));
                return true;
            }
        }
        return false;
    }

    public BlockingQueue<ImmutablePair<String, Long>> getTimestampMap() {
        return timestampMap;
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
