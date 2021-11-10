package common.tuple;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import stream.WMStreamProcessingContext;

import java.util.UUID;
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
    private BlockingQueue<ImmutableTriple<String, String, Long>> timestampMap;

    protected final boolean watermark;

    private final UUID tupleId;

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
        if (WMStreamProcessingContext.getContext().getDebugLevel() >= WMStreamProcessingContext.DEBUG_LEVEL_BASIC) {
            this.tupleId = UUID.randomUUID();
            if (WMStreamProcessingContext.getContext().getDebugLevel() >=
                WMStreamProcessingContext.DEBUG_LEVEL_MODERATE &&
                watermark) {
                this.timestampMap = new LinkedBlockingQueue<>();
            }
        } else {
            this.tupleId = null;
        }
    }

    public boolean addTimestampMarker(String operatorId, Long timestamp) {
        if (WMStreamProcessingContext.getContext().getDebugLevel() >= WMStreamProcessingContext.DEBUG_LEVEL_MODERATE &&
            this.watermark) {
            if (this.timestampMap.stream().anyMatch(tsMarker -> tsMarker.getMiddle().equals(operatorId))) {
                return false;
            } else {
                this.timestampMap.add(
                    new ImmutableTriple<>(Thread.currentThread().getName(), operatorId,
                        timestamp));
                return true;
            }
        }
        return false;
    }

    public BlockingQueue<ImmutableTriple<String, String, Long>> getTimestampMap() {
        return timestampMap;
    }

    public boolean isWatermark() {
        return watermark;
    }

    public String getTupleId() {
        return tupleId.toString();
    }

    @Override
    public String toString() {
        return timestamp +
            ", " + endTimestamp +
            ", " + stimulus +
            ", " + key +
            ", " + value.replaceAll(",", "|") +
            ", " + watermark +
            ", " + latency +
            ", " + String.format("%.3f", throughput);
    }
}
