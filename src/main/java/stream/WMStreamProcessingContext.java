package stream;

import common.tuple.WatermarkedBaseRichTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WMStreamProcessingContext {
    private static WMStreamProcessingContext wmStreamProcessingContext = null;

    private final Map<String, PriorityBasedStream<? extends WatermarkedBaseRichTuple>> streamIdObjMap =
        new ConcurrentHashMap<>();

    private WMStreamProcessingContext() {

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

    public void processWatermarkArrival(Long watermarkTimestamp) {
        streamIdObjMap.values()
            .forEach(priorityBasedStream -> priorityBasedStream.extractHighPriorityEvents(watermarkTimestamp));
    }
}
