package stream;

import common.tuple.WatermarkedBaseRichTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WMStreamProcessingContext {
    public static final int DEBUG_LEVEL_OFF = 0;
    public static final int DEBUG_LEVEL_BASIC = 1;
    public static final int DEBUG_LEVEL_MODERATE = 2;
    public static final int DEBUG_LEVEL_FULL = 4;
    public static final int DEBUG_LEVEL_OVERDRIVE = 8;

    private static WMStreamProcessingContext wmStreamProcessingContext = null;

    private int debugLevel;

    private final Map<String, PriorityBasedStream<? extends WatermarkedBaseRichTuple>> streamIdObjMap =
        new ConcurrentHashMap<>();

    private WMStreamProcessingContext() {
        this.debugLevel = DEBUG_LEVEL_MODERATE;
    }

    public static WMStreamProcessingContext getContext() {
        if (wmStreamProcessingContext == null) {
            wmStreamProcessingContext = new WMStreamProcessingContext();
        }

        return wmStreamProcessingContext;
    }

    public void addStream(PriorityBasedStream<? extends WatermarkedBaseRichTuple> stream) {
        streamIdObjMap.put(stream.getId(), stream);
    }

    public PriorityBasedStream<? extends WatermarkedBaseRichTuple> getStream(String streamId) {
        return streamIdObjMap.get(streamId);
    }

    public List<PriorityBasedStream<? extends WatermarkedBaseRichTuple>> getStreamList() {
        return new ArrayList<>(streamIdObjMap.values());
    }

    public int getDebugLevel() {
        return debugLevel;
    }

    public void setDebugLevel(int debugLevel) {
        this.debugLevel = debugLevel;
    }

    public void processWatermarkArrival(Long watermarkTimestamp) {
        streamIdObjMap.values()
            .forEach(priorityBasedStream -> priorityBasedStream.extractHighPriorityEvents(watermarkTimestamp));
    }

    public boolean areAllHighPriorityStreamsEmpty() {
        return streamIdObjMap.values().stream()
            .allMatch(priorityBasedStream -> priorityBasedStream.getHighPrioritySize() == 0);
    }
}
