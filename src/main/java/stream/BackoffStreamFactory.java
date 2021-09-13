package stream;

import common.tuple.RichTuple;
import component.StreamConsumer;
import component.StreamProducer;
import common.util.backoff.Backoff;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BackoffStreamFactory implements StreamFactory {

	private final AtomicInteger indexes = new AtomicInteger();

	@Override
	public <T> Stream<T> newStream(StreamProducer<T> from,
			StreamConsumer<T> to,
			int capacity, Backoff backoff) {
		return new BackoffStream<>(
				getStreamId(from, to), indexes.getAndIncrement(), from, to, capacity, backoff);
	}

	@Override
	public <T extends RichTuple> Stream<T> newPriorityBasedStream(StreamProducer<T> from, StreamConsumer<T> to,
																  int capacity, Backoff backoff) {
		throw new UnsupportedOperationException("Priority based streams are not supported for class " + getClass().getCanonicalName());
	}

	@Override
	public <T extends Comparable<? super T>> MWMRStream<T> newMWMRStream(
			List<? extends StreamProducer<T>> sources, List<? extends StreamConsumer<T>> destinations,
			int maxLevels) {
		// TODO Ugly to get index 0 by default?
		return new SGStream<T>(getStreamId(sources.get(0), destinations.get(0)),
				indexes.getAndIncrement(),
				maxLevels, sources.size(),
				destinations.size(), sources, destinations);
	}

}
