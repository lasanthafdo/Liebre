package stream;

import common.tuple.RichTuple;
import common.util.backoff.Backoff;
import component.StreamConsumer;
import component.StreamProducer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PriorityBasedBackoffStreamFactory implements StreamFactory {

	private final AtomicInteger indexes = new AtomicInteger();

	@Override
	public <T> Stream<T> newStream(StreamProducer<T> from,
			StreamConsumer<T> to,
			int capacity, Backoff backoff) {
		throw new UnsupportedOperationException("Stream should be created using newPriorityBasedStream() method");
	}

	@Override
	public <RT extends RichTuple> Stream<RT> newPriorityBasedStream(StreamProducer<RT> from,
																	StreamConsumer<RT> to,
																	int capacity, Backoff backoff) {
		return new PriorityBasedBackoffStream<>(
			getStreamId(from, to), indexes.getAndIncrement(), from, to, capacity, backoff);
	}

	@Override
	public <T extends Comparable<? super T>> MWMRStream<T> newMWMRStream(
			List<? extends StreamProducer<T>> sources, List<? extends StreamConsumer<T>> destinations,
			int maxLevels) {
		throw new UnsupportedOperationException("MWMRStream creation is not supported for priority based backoff streams");
	}

}
