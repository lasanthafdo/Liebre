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

package query;

import common.tuple.RichTuple;
import common.tuple.Tuple;
import common.util.backoff.BackoffFactory;
import common.util.backoff.ExponentialBackoff;
import component.Component;
import component.StreamConsumer;
import component.StreamProducer;
import component.operator.Operator;
import component.operator.in1.Operator1In;
import component.operator.in1.Operator1InStatistic;
import component.operator.in1.aggregate.TimeBasedSingleWindow;
import component.operator.in1.aggregate.TimeBasedSingleWindowAggregate;
import component.operator.in1.filter.FilterFunction;
import component.operator.in1.filter.FilterOperator;
import component.operator.in1.map.FlatMapFunction;
import component.operator.in1.map.FlatMapOperator;
import component.operator.in1.map.MapFunction;
import component.operator.in1.map.MapOperator;
import component.operator.in2.Operator2In;
import component.operator.in2.Operator2InStatistic;
import component.operator.in2.join.JoinFunction;
import component.operator.in2.join.TimeBasedJoin;
import component.operator.router.BaseRouterOperator;
import component.operator.router.RouterOperator;
import component.operator.router.RouterOperatorStatistic;
import component.operator.union.UnionOperator;
import component.sink.BaseSink;
import component.sink.Sink;
import component.sink.SinkFunction;
import component.sink.SinkStatistic;
import component.sink.TextFileSink;
import component.sink.TextSinkFunction;
import component.source.BaseSource;
import component.source.Source;
import component.source.SourceFunction;
import component.source.SourceStatistic;
import component.source.TextFileSource;
import component.source.TextSourceFunction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;
import scheduling.impl.DefaultScheduler;
import stream.BlockingStream;
import stream.Stream;
import stream.StreamFactory;
import stream.StreamStatistic;

/**
 * The main execution unit. Acts as a factory for the stream {@link Component}s such as {@link
 * Operator}s, {@link Source}s and {@link Sink}s through various helper methods. It also handles the
 * connections of the components with the correct types of {@link Stream}s and the
 * activation/deactivation of the query. Activating the query also starts executing it by delegating
 * this work to the provided {@link Scheduler} implementation.
 */
public final class Query {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final int DEFAULT_STREAM_CAPACITY = 10000;
  private final Map<String, Operator<? extends Tuple, ? extends Tuple>> operators = new HashMap<>();
  private final Map<String, Source<? extends Tuple>> sources = new HashMap<>();
  private final Map<String, Sink<? extends Tuple>> sinks = new HashMap<>();
  private final Scheduler scheduler;
  private final Map<StatisticType, StatisticsConfiguration> enabledStatistics = new HashMap<>();
  private final StreamFactory streamFactory;
  private BackoffFactory defaultBackoff = BackoffFactory.NOOP;
  private boolean active;

  /**
   * Construct.
   */
  public Query() {
    this(new DefaultScheduler(), BlockingStream.factory());
  }

  /**
   * Construct.
   *
   * @param scheduler The scheduler implementation to use when executing the query after Query{@link
   * #activate()} is called.
   */
  public Query(Scheduler scheduler, StreamFactory streamFactory) {
    this.scheduler = scheduler;
    // Set a default backoff value
    setBackoff(1, 20, 5);
    this.streamFactory = streamFactory;
  }

  /**
   * Activate all statistics.
   *
   * @param statisticsFolder The folder to save the statistics to.
   */
  public synchronized void activateStatistics(String statisticsFolder) {
    activateStatistics(statisticsFolder, true);
  }

  /**
   * Activate all statistics.
   *
   * @param statisticsFolder The folder to save the statistics to.
   * @param autoFlush Control whether the file bufferrs are autoFlushed or not.
   */
  public synchronized void activateStatistics(String statisticsFolder, boolean autoFlush) {
    for (StatisticType type : StatisticType.values()) {
      activateStatistic(statisticsFolder, autoFlush, type);
    }
  }


  /**
   * Activate a specific statistic.
   *
   * @param statisticsFolder The folder to save the statistics to.
   * @param autoFlush Control whether the file bufferrs are autoFlushed or not.
   * @param type The type of statistic to activate.
   */
  public synchronized void activateStatistic(String statisticsFolder, boolean autoFlush,
      StatisticType type) {
    Validate
        .isTrue(!enabledStatistics.containsKey(type), "Statistics for %s already enabled", type);
    LOGGER.info("Enabling statistics for {}", type.name().toLowerCase());
    enabledStatistics.put(type, new StatisticsConfiguration(statisticsFolder, autoFlush));
  }


  /**
   * Set the parameters for the default {@link ExponentialBackoff} strategy.
   * @param min The minimum backoff limit
   * @param max The maximum backoff limit
   * @param retries The number of retries before the backoff limit is updated.
   */
  public synchronized void setBackoff(int min, int max, int retries) {
    this.defaultBackoff = ExponentialBackoff.factory(min, max, retries);
  }

  public synchronized void setBackoff(BackoffFactory backoffFactory) {
    this.defaultBackoff = backoffFactory;
  }

  public synchronized <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addOperator(
      Operator1In<IN, OUT> operator) {
    Operator<IN, OUT> decoratedOperator = operator;
    if (enabledStatistics.containsKey(StatisticType.OPERATORS)) {
      StatisticsConfiguration statConfig = enabledStatistics.get(StatisticType.OPERATORS);
      decoratedOperator = new Operator1InStatistic<IN, OUT>(operator, statConfig.folder(),
          statConfig.autoFlush());
    }
    saveComponent(operators, decoratedOperator, "component/operator");
    return decoratedOperator;
  }

  public synchronized <IN extends RichTuple, OUT extends RichTuple> Operator<IN, OUT> addAggregateOperator(
      String identifier,
      TimeBasedSingleWindow<IN, OUT> window, long windowSize, long windowSlide) {

    return addOperator(
        new TimeBasedSingleWindowAggregate<IN, OUT>(identifier, windowSize,
            windowSlide, window));
  }

  public synchronized <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addMapOperator(
      String identifier,
      MapFunction<IN, OUT> mapFunction) {
    return addOperator(new MapOperator<IN, OUT>(identifier, mapFunction));
  }

  public synchronized <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addFlatMapOperator(
      String identifier,
      FlatMapFunction<IN, OUT> mapFunction) {
    return addOperator(new FlatMapOperator<IN, OUT>(identifier, mapFunction));
  }

  public synchronized <T extends Tuple> Operator<T, T> addFilterOperator(String identifier,
      FilterFunction<T> filterF) {
    return addOperator(new FilterOperator<T>(identifier, filterF));
  }

  public synchronized <T extends Tuple> RouterOperator<T> addRouterOperator(String identifier) {
    RouterOperator<T> router = new BaseRouterOperator<T>(identifier);
    if (enabledStatistics.containsKey(StatisticType.OPERATORS)) {
      StatisticsConfiguration statConfig = enabledStatistics.get(StatisticType.OPERATORS);
      router = new RouterOperatorStatistic<T>(router, statConfig.folder(), statConfig.autoFlush());
    }
    saveComponent(operators, router, "component/operator");
    return router;
  }

  public synchronized <T extends Tuple> UnionOperator<T> addUnionOperator(UnionOperator<T> union) {
    saveComponent(operators, union, "component/operator");
    return union;
  }

  public synchronized <T extends Tuple> UnionOperator<T> addUnionOperator(String identifier) {
    UnionOperator<T> union = new UnionOperator<>(identifier);
    return addUnionOperator(union);
  }

  public synchronized <T extends Tuple> Source<T> addSource(Source<T> source) {
    Source<T> decoratedSource = source;
    if (enabledStatistics.containsKey(StatisticType.SOURCES)) {
      StatisticsConfiguration statConfig = enabledStatistics.get(StatisticType.SOURCES);
      decoratedSource = new SourceStatistic<T>(decoratedSource, statConfig.folder(),
          statConfig.autoFlush());
    }
    saveComponent(sources, decoratedSource, "component/source");
    return decoratedSource;
  }

  public synchronized <T extends Tuple> Source<T> addBaseSource(String id,
      SourceFunction<T> function) {
    return addSource(new BaseSource<>(id, function));
  }

  public synchronized <T extends Tuple> Source<T> addTextFileSource(String id, String filename,
      TextSourceFunction<T> function) {
    return addSource(new TextFileSource(id, filename, function));
  }

  public synchronized <T extends Tuple> Sink<T> addSink(Sink<T> sink) {
    Sink<T> decoratedSink = sink;
    if (enabledStatistics.containsKey(StatisticType.SINKS)) {
      StatisticsConfiguration statConfig = enabledStatistics.get(StatisticType.SINKS);
      decoratedSink = new SinkStatistic<T>(sink, statConfig.folder(), statConfig.autoFlush());
    }
    saveComponent(sinks, decoratedSink, "component/sink");
    return decoratedSink;
  }

  public synchronized <T extends Tuple> Sink<T> addBaseSink(String id,
      SinkFunction<T> sinkFunction) {
    return addSink(new BaseSink<>(id, sinkFunction));
  }

  public synchronized <T extends Tuple> Sink<T> addTextFileSink(String id, String file,
      TextSinkFunction<T> function) {
    return addSink(new TextFileSink<>(id, file, function));
  }

  public synchronized <OUT extends Tuple, IN extends Tuple, IN2 extends Tuple> Operator2In<IN,
      IN2, OUT> addOperator2In(
      Operator2In<IN, IN2, OUT> operator) {
    Operator2In<IN, IN2, OUT> decoratedOperator = operator;
    if (enabledStatistics.containsKey(StatisticType.OPERATORS)) {
      StatisticsConfiguration statConfig = enabledStatistics.get(StatisticType.OPERATORS);
      decoratedOperator = new Operator2InStatistic<IN, IN2, OUT>(operator, statConfig.folder(),
          statConfig.autoFlush());
    }
    saveComponent(operators, decoratedOperator, "operator2in");
    return decoratedOperator;
  }

  public synchronized <IN extends RichTuple, IN2 extends RichTuple, OUT extends RichTuple> Operator2In<IN, IN2, OUT> addJoinOperator(
      String identifier, JoinFunction<IN, IN2, OUT> joinFunction, long windowSize) {
    return addOperator2In(
        new TimeBasedJoin<IN, IN2, OUT>(identifier, windowSize, joinFunction));
  }

  public synchronized <T extends Tuple> Query connect(StreamProducer<T> source,
      StreamConsumer<T> destination) {
    return connect(source, destination, defaultBackoff);
  }

  public synchronized <T extends Tuple> Query connect(StreamProducer<T> source,
      StreamConsumer<T> destination,
      BackoffFactory backoff) {
    Validate.isTrue(destination instanceof Operator2In == false,
        "Error when connecting '%s': Please use connect2inXX() for Operator2In and subclasses!",
        destination.getId());
    Stream<T> stream = getStream(source, destination, backoff);
    source.addOutput(destination, stream);
    destination.addInput(source, stream);
    return this;
  }

  public synchronized <T extends Tuple> Query connect2inLeft(StreamProducer<T> source,
      Operator2In<T, ?, ?> destination) {
    return connect2inLeft(source, destination, defaultBackoff);
  }

  public synchronized <T extends Tuple> Query connect2inLeft(StreamProducer<T> source,
      Operator2In<T, ?, ?> destination, BackoffFactory backoff) {
    Stream<T> stream = getStream(source, destination, backoff);
    source.addOutput(destination, stream);
    destination.addInput(source, stream);
    return this;
  }

  public synchronized <T extends Tuple> Query connect2inRight(StreamProducer<T> source,
      Operator2In<?, T, ?> destination) {
    return connect2inRight(source, destination, defaultBackoff);
  }

  public synchronized <T extends Tuple> Query connect2inRight(StreamProducer<T> source,
      Operator2In<?, T, ?> destination, BackoffFactory backoff) {
    Stream<T> stream = getStream(source, destination.secondInputView(), backoff);
    source.addOutput(destination.secondInputView(), stream);
    destination.addInput2(source, stream);
    return this;
  }

  private synchronized <T extends Tuple> Stream<T> getStream(StreamProducer<T> source,
      StreamConsumer<T> destination,
      BackoffFactory backoff) {
    Stream<T> stream = streamFactory
        .newStream(source, destination, DEFAULT_STREAM_CAPACITY, backoff);
    System.out.println("Generated stream " + stream);
    if (enabledStatistics.containsKey(StatisticType.STREAMS)) {
      StatisticsConfiguration statConfig = enabledStatistics.get(StatisticType.STREAMS);
      return new StreamStatistic<>(stream, statConfig.folder(), statConfig.autoFlush());
    } else {
      return stream;
    }
  }

  /**
   * Activate and start executing the query.
   */
  public synchronized void activate() {

    LOGGER.info("Activating query...");
    LOGGER.info("Components: {} Sources, {} Operators, {} Sinks, {} Streams", sources.size(),
        operators.size(), sinks.size(), streams().size());
    scheduler.addTasks(sinks.values());
    scheduler.addTasks(operators.values());
    scheduler.addTasks(sources.values());
    scheduler.enable();
    scheduler.startTasks();
    active = true;
  }

  /**
   * Deactivate and stop executing the query.
   */
  public synchronized void deActivate() {
    if (!active) {
      return;
    }
    LOGGER.info("Deactivating query...");
    scheduler.disable();
    LOGGER.info("Waiting for threads to terminate...");
    scheduler.stopTasks();
    LOGGER.info("DONE!");
    active = false;
  }

  /**
   * Get the number of sources in the query.
   * @return The number of sources.
   */
  public int sourcesNumber() {
    return sources.size();
  }

  private Set<Stream<?>> streams() {
    Set<Stream<?>> streams = new HashSet<>();
    for (Operator<?, ?> op : operators.values()) {
      streams.addAll(op.getInputs());
    }
    return streams;
  }

  private <T extends Component> void saveComponent(Map<String, T> map, T component, String type) {
    Validate.validState(!map.containsKey(component.getId()),
        "A component of type %s  with id '%s' has already been added!", type, component);
    Validate.notNull(component);
    if (component.getId().contains("_")) {
      LOGGER.warn(
          "It is best to avoid component IDs that contain an underscore because it will make it more difficult to analyze statistics date. Offending component: {}",
          component);
    }
    map.put(component.getId(), component);
  }


}
