package stream;

import common.tuple.WatermarkedBaseRichTuple;
import common.util.backoff.Backoff;
import component.Component;
import component.StreamConsumer;
import component.StreamProducer;
import scheduling.LiebreScheduler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PriorityBasedStreamFactory implements StreamFactory {

    private final AtomicInteger indexes = new AtomicInteger();
    private final LiebreScheduler<? extends Component> scheduler;

    public PriorityBasedStreamFactory(LiebreScheduler<? extends Component> scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public <T> Stream<T> newStream(StreamProducer<T> from,
                                   StreamConsumer<T> to,
                                   int capacity, Backoff backoff) {
        throw new UnsupportedOperationException("Stream should be created using newPriorityBasedStream() method");
    }

    @Override
    public <WT extends WatermarkedBaseRichTuple> Stream<WT> newPriorityBasedStream(StreamProducer<WT> from,
                                                                                   StreamConsumer<WT> to,
                                                                                   int capacity, Backoff backoff) {
        PriorityBasedStream<WT> priorityBasedStream = new PriorityBasedStream<>(
            getStreamId(from, to), indexes.getAndIncrement(), from, to, capacity, scheduler);
        WMStreamProcessingContext.getContext().addStream(priorityBasedStream);
        return priorityBasedStream;
    }

    @Override
    public <T extends Comparable<? super T>> MWMRStream<T> newMWMRStream(
        List<? extends StreamProducer<T>> sources, List<? extends StreamConsumer<T>> destinations,
        int maxLevels) {
        throw new UnsupportedOperationException(
            "MWMRStream creation is not supported for priority based backoff streams");
    }

}
