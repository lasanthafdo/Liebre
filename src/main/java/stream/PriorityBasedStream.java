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
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final WatermarkedBaseRichTuple
        BEING_PROCESSED_MARKER = new WatermarkedBaseRichTuple(0L, "MARKER", "MARKER", true);

    private static final BlockingQueue<WatermarkedBaseRichTuple> globalPendingWatermarks =
        new PriorityBlockingQueue<>(1000, Comparator.comparingLong(
            RichTuple::getTimestamp));
    private static WatermarkedBaseRichTuple globalCurrentWatermark;
    private static Long globalWatermarkTs = 0L;
    private static final AtomicReference<WatermarkedBaseRichTuple> globalWatermarkRef = new AtomicReference<>();
    private static final Map<String, Set<WatermarkedBaseRichTuple>> incomingEventHistory = new ConcurrentHashMap<>();
    private static final Map<String, Set<WatermarkedBaseRichTuple>> outgoingEventHistory = new ConcurrentHashMap<>();

    private final Queue<T> stream = new ConcurrentLinkedQueue<>();
    private final Queue<T> highPriorityStream = new ConcurrentLinkedQueue<>();
    private final int capacity;
    private final StreamProducer<T> source;
    private final StreamConsumer<T> destination;
    private final AtomicLong tuplesRead = new AtomicLong(0);
    private final AtomicLong tuplesWritten = new AtomicLong(0);
    private final AtomicLong highPriorityTuplesRead = new AtomicLong(0);
    private final AtomicLong highPriorityTuplesWritten = new AtomicLong(0);
    private final AtomicBoolean streamPrioritized = new AtomicBoolean(false);
    private final LiebreScheduler<? extends Component> scheduler;

    private volatile boolean isFlushed = false;
    private volatile double averageArrivalTime = -1;

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
            if (WMStreamProcessingContext.getContext().getDebugLevel() >= WMStreamProcessingContext.DEBUG_LEVEL_FULL) {
                Set<WatermarkedBaseRichTuple> tupleIdSet = incomingEventHistory.computeIfAbsent(source.getId(),
                    operatorId -> new ConcurrentSkipListSet<>(
                        Comparator.comparing(WatermarkedBaseRichTuple::getTupleId)));
                if (!tupleIdSet.add(tuple)) {
                    throw new IllegalStateException(
                        "Same input tuple with " + tuple.getTupleId() + " is being put to the same stream");
                }
            }
            if (tuple.isWatermark() && tuple.getTimestamp() > globalWatermarkTs &&
                source instanceof Source) { // is a later watermark
                processGlobalWatermarkArrival(tuple);
                scheduler.scheduleTasks();
            }
            if (tuple.getTimestamp() <= globalWatermarkTs) {
                highPriorityStream.offer(tuple);
                highPriorityTuplesWritten.incrementAndGet();
            } else {
                stream.offer(tuple);
            }
        }
        tuplesWritten.incrementAndGet();
        return true;
    }

    private void processGlobalWatermarkArrival(T tuple) {
        if (globalCurrentWatermark != null) {
            globalPendingWatermarks.add(tuple);
        } else {
            WMStreamProcessingContext.getContext().processWatermarkArrival(tuple.getTimestamp());
            globalCurrentWatermark = tuple;
            globalWatermarkTs = globalCurrentWatermark.getTimestamp();
            globalWatermarkRef.set(globalCurrentWatermark);
        }
    }

    private void processWatermark() {
        if (WMStreamProcessingContext.getContext().areAllHighPriorityStreamsEmpty()) {
            // Weee! Watermark can be processed
            globalCurrentWatermark = globalPendingWatermarks.poll();
            if (globalCurrentWatermark != null) {
                globalWatermarkTs = globalCurrentWatermark.getTimestamp();
                WMStreamProcessingContext.getContext().processWatermarkArrival(globalWatermarkTs);
            }
        }
        globalWatermarkRef.set(globalCurrentWatermark);
    }

    public synchronized void extractHighPriorityEvents(Long watermarkTimestamp) {
        stream.stream().filter(tuple -> tuple.getTimestamp() < watermarkTimestamp)
            .forEachOrdered(highPriorityStream::offer);
        stream.removeIf(tuple -> tuple.getTimestamp() < watermarkTimestamp);
        streamPrioritized.set(true);
    }

    @Override
    public T doGetNextTuple(int consumerIndex) {
        T tuple = highPriorityStream.poll();
        if (tuple == null) {
            streamPrioritized.compareAndSet(true, false);
            if (globalCurrentWatermark != null && (destination instanceof Sink)) {
                if (globalWatermarkRef.compareAndSet(globalCurrentWatermark, BEING_PROCESSED_MARKER)) {
                    processWatermark();
                } else {
                    while (BEING_PROCESSED_MARKER.equals(globalCurrentWatermark)) {
                        Thread.yield();
                    }
                }
            }
            tuple = stream.poll();
        } else {
            highPriorityTuplesRead.incrementAndGet();
        }
        if (tuple != null) {
            tuplesRead.incrementAndGet();
        }
        if (WMStreamProcessingContext.getContext().getDebugLevel() >= WMStreamProcessingContext.DEBUG_LEVEL_FULL &&
            tuple != null) {
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
        return stream.size();
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

    public boolean isStreamPrioritized() {
        return streamPrioritized.get();
    }
}
