/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package stream;

import common.tuple.RichTuple;
import common.tuple.WatermarkedBaseRichTuple;
import common.util.backoff.Backoff;
import common.util.backoff.ExponentialBackoff;
import common.util.backoff.InactiveBackoff;
import component.Component;
import component.StreamConsumer;
import component.StreamProducer;
import component.sink.Sink;
import component.source.Source;
import org.apache.commons.lang3.builder.ToStringBuilder;
import scheduling.LiebreScheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stream implementation that has an (optional) approximate capacity that is enforced by an optional
 * provided {@link Backoff} strategy. The backoff strategy is also activated in case the reader is
 * faster than the writer, to prevent spinning.
 *
 * @param <T> The type of tuples transferred by this {@link Stream}.
 * @see StreamFactory
 * @see ExponentialBackoff
 * @see InactiveBackoff
 */
public class PriorityBasedStream<T extends WatermarkedBaseRichTuple> extends AbstractStream<T> {

    private static final double MOVING_AVG_ALPHA = 0.5;
    private static final WatermarkedBaseRichTuple
        BEING_PROCESSED_MARKER = new WatermarkedBaseRichTuple(0L, "MARKER", "MARKER", true);

    private final Queue<T> stream = new ConcurrentLinkedQueue<>();
    private final Queue<T> highPriorityStream = new ConcurrentLinkedQueue<>();
    private final int capacity;
    private final StreamProducer<T> source;
    private final StreamConsumer<T> destination;
    private volatile boolean isFlushed = false;
    private volatile double averageArrivalTime = -1;
    private final AtomicLong tuplesRead = new AtomicLong(0);
    private final AtomicLong tuplesWritten = new AtomicLong(0);
    private final AtomicLong highPriorityTuplesRead = new AtomicLong(0);
    private final AtomicLong highPriorityTuplesWritten = new AtomicLong(0);
    private final LiebreScheduler<? extends Component> scheduler;

    private static final BlockingQueue<WatermarkedBaseRichTuple> globalPendingWatermarks =
        new PriorityBlockingQueue<>(1000, Comparator.comparingLong(
            RichTuple::getTimestamp));
    private static WatermarkedBaseRichTuple globalCurrentWatermark;
    private static Long globalWatermarkTs = 0L;
    private static Long lastGlobalWatermarkTs = 0L;

    private final BlockingQueue<WatermarkedBaseRichTuple> pendingWatermarks =
        new PriorityBlockingQueue<>(1000, Comparator.comparingLong(
            RichTuple::getTimestamp));
    private final AtomicReference<WatermarkedBaseRichTuple> currentWatermarkRef = new AtomicReference<>();
    private WatermarkedBaseRichTuple currentWatermark;

    private int lastStreamSize = 0;

    private static final Map<String, Set<WatermarkedBaseRichTuple>> incomingEventHistory = new ConcurrentHashMap<>();
    private static final Map<String, Set<WatermarkedBaseRichTuple>> outgoingEventHistory = new ConcurrentHashMap<>();

    /**
     * Construct.
     *
     * @param id          The unique ID of the stream.
     * @param index       The unique index of the stream.
     * @param source      The producer
     * @param destination The consumer
     * @param capacity    The capacity that the stream will try to enforce with the {@link Backoff}
     *                    strategy.
     */
    PriorityBasedStream(
        String id,
        int index,
        StreamProducer<T> source,
        StreamConsumer<T> destination,
        int capacity,
        LiebreScheduler<? extends Component> scheduler) {
        super(id, index);
        this.capacity = capacity;
        this.source = source;
        this.destination = destination;
        this.scheduler = scheduler;
    }

    @Override
    public void doAddTuple(T tuple, int producerIndex) {
        offer(tuple, producerIndex);
    }

    @Override
    public final boolean offer(T tuple, int producerIndex) {
        if (tuple == null) {
            return false;
        } else {
            Set<WatermarkedBaseRichTuple> tupleIdSet = incomingEventHistory.computeIfAbsent(source.getId(),
                operatorId -> new ConcurrentSkipListSet<>(Comparator.comparing(WatermarkedBaseRichTuple::getTupleId)));
            if (!tupleIdSet.add(tuple)) {
                throw new IllegalStateException(
                    "Same input tuple with " + tuple.getTupleId() + " is being put to the same stream");
            }

            if (tuple.getTimestamp() > lastGlobalWatermarkTs) {
                if (tuple.isWatermark()) { // is a watermark
                    if (tuple.getTimestamp() > globalWatermarkTs) { // is a later watermark
                        if (source instanceof Source) {
                            processGlobalWatermarkArrival(tuple);
                            scheduler.scheduleTasks();
                        }
                        if (currentWatermark != null) {
                            pendingWatermarks.add(tuple);
                        } else {
                            currentWatermark = tuple;
                            currentWatermarkRef.set(currentWatermark);
                        }
                    } else {
                        // older or same watermark is ignored
                        return true;
                    }
                } else if (tuple.getTimestamp() <= globalWatermarkTs) {
                    highPriorityStream.offer(tuple);
                    highPriorityTuplesWritten.incrementAndGet();
                } else {
                    stream.offer(tuple);
                }
            } else {
                // else the tuple gets dropped
                return true;
            }
        }
        tuplesWritten.incrementAndGet();
        return true;
    }

    private void processGlobalWatermarkArrival(T tuple) {
        if (globalCurrentWatermark != null) {
            globalPendingWatermarks.add(tuple);
        } else {
            globalCurrentWatermark = tuple;
            globalWatermarkTs = globalCurrentWatermark.getTimestamp();
            WMStreamProcessingContext.getContext().processWatermarkArrival(globalWatermarkTs);
        }
    }

    private T processWatermark() {
        T processedWatermark = null;
        if (highPriorityStream.isEmpty()) {
            // Weee! Watermark can be processed
            processedWatermark = (T) currentWatermark;
            currentWatermark = pendingWatermarks.poll();
            if (destination instanceof Sink) {
                lastGlobalWatermarkTs = globalWatermarkTs;
                globalCurrentWatermark = globalPendingWatermarks.poll();
                if (globalCurrentWatermark != null) {
                    globalWatermarkTs = globalCurrentWatermark.getTimestamp();
                    WMStreamProcessingContext.getContext().processWatermarkArrival(globalWatermarkTs);
                }
            }
        }
        currentWatermarkRef.set(currentWatermark);
        return processedWatermark;
    }

    public synchronized void extractHighPriorityEvents(Long watermarkTimestamp) {
        stream.stream().filter(tuple -> tuple.getTimestamp() < watermarkTimestamp)
            .forEachOrdered(highPriorityStream::offer);
        stream.removeIf(tuple -> tuple.getTimestamp() < watermarkTimestamp);
    }

    @Override
    public T doGetNextTuple(int consumerIndex) {
        T tuple = highPriorityStream.poll();
        if (tuple == null) {
            if (currentWatermark != null) {
                if (currentWatermarkRef.compareAndSet(currentWatermark, BEING_PROCESSED_MARKER)) {
                    tuple = processWatermark();
                    assert !currentWatermark.equals(BEING_PROCESSED_MARKER);
                } // Otherwise, tuple is already set to null
            } else {
                tuple = stream.poll();
            }
        } else {
            highPriorityTuplesRead.incrementAndGet();
        }
        if (tuple != null) {
            tuplesRead.incrementAndGet();
        }
        if (tuple != null) {
            Set<WatermarkedBaseRichTuple> tupleIdSet = outgoingEventHistory.computeIfAbsent(destination.getId(),
                operatorId -> new ConcurrentSkipListSet<>(Comparator.comparing(WatermarkedBaseRichTuple::getTupleId)));
            if (!tupleIdSet.add(tuple)) {
                throw new IllegalStateException(
                    "Same output tuple with ID " + tuple.getTupleId() + " is being extracted by the same stream");
            }
        }
        return tuple;
    }

    @Override
    public final T peek(int consumerIndex) {
        return stream.peek();
    }

    @Override
    public final int remainingCapacity() {
        return Math.max(capacity - size(), 0);
    }

    @Override
    public final int size() {
        int streamSize =
            (int) Math.round(movingAverage(stream.size() + getHighPrioritySize(), lastStreamSize));
        lastStreamSize = streamSize;
        return streamSize;
    }

    @Override
    public int getHighPrioritySize() {
        return highPriorityStream.size();
    }

    @Override
    public List<? extends StreamProducer<T>> producers() {
        return Collections.singletonList(source);
    }

    @Override
    public List<? extends StreamConsumer<T>> consumers() {
        return Collections.singletonList(destination);
    }

    @Override
    public void resetArrivalTime() {
        averageArrivalTime = -1;
    }

    @Override
    public double averageArrivalTime() {
        return averageArrivalTime;
    }

    @Override
    public void flush() {
        isFlushed = true;
    }

    @Override
    public boolean isFlushed() {
        return isFlushed;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("id", id)
            .append("index", index)
            .append("capacity", capacity)
            .append("size", size())
            .append("component/source", source)
            .append("destination", destination)
            .append("enabled", enabled)
            .toString();
    }

    private double movingAverage(double newValue, double oldValue) {
        return (MOVING_AVG_ALPHA * newValue) + ((1 - MOVING_AVG_ALPHA) * oldValue);
    }
}
