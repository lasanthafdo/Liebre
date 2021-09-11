package component;

import common.metrics.Metric;
import common.metrics.TimeMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import query.LiebreContext;
import stream.Stream;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

public abstract class AbstractComponent<IN, OUT> implements Component {

  private static final Logger LOG = LogManager.getLogger();
  protected final ComponentState<IN, OUT> state;

  // Exponential moving average alpha parameter
  // for cost and selectivity moving averages
  // The ALPHA we want to use for EMAs
  private static final double TARGET_ALPHA = 0.3;
  // The update period that the target alpha would be applied to, in millis
  private static final long TARGET_UPDATE_PERIOD = 1000;
  private static final long MILLIS_TO_NANOS = 1000000;
  // The actual alpha that we use, changing depending on the actual update period length
  private volatile double alpha = 0.2;
  private final LongAdder tuplesWritten = new LongAdder();
  private final LongAdder tuplesRead = new LongAdder();
  private final LongAdder processingTimeNanos = new LongAdder();
  private volatile long lastUpdateTime = System.currentTimeMillis();
  private volatile double selectivity = 1;
  private final Set<Integer> replicaSet = new HashSet<>();

  private volatile double cost = 1;
  private volatile double rate = 0;

  private final TimeMetric executionTimeMetric;
  private final Metric rateMetric;
  private boolean flushed;

  public AbstractComponent(String id, ComponentType type) {
    this.state = new ComponentState<>(id, type);
    this.executionTimeMetric =
        LiebreContext.operatorMetrics().newAverageTimeMetric(id, "EXEC");
    this.rateMetric = LiebreContext.operatorMetrics().newCountPerSecondMetric(id, "RATE");
  }

  @Override
  public boolean runFor(int rounds) {
    int executions = 0;
    long tuplesWrittenBefore = tuplesWritten.longValue();
    long tuplesReadBefore = tuplesRead.longValue();
    long startTime = System.nanoTime();
    // Process while the component is enabled, the rounds are not finished and there is actually
    // processing happening at every execution
    while (isEnabled() && executions < rounds) {
      run();
      executions += 1;
    }
    long endTime = System.nanoTime();
    // Update processing time
    processingTimeNanos.add(endTime - startTime);
    return tuplesReadBefore != tuplesRead.longValue() || tuplesWrittenBefore != tuplesWritten.longValue();
  }

  @Override
  public final void run() {
    if (isEnabled()) {
      executionTimeMetric.startInterval();
      process();
      executionTimeMetric.stopInterval();
    }
  }

  protected abstract void process();

  protected final void increaseTuplesRead() {
    tuplesRead.increment();
    rateMetric.record(1);
  }

  protected final void increaseTuplesWritten() {
    tuplesWritten.increment();
  }

  /**
   * Update the cost and selectivity based on the tuples processed and the time it took. <br>
   * <b>WARNING: The variables for the metrics are available only the execution happens with {@link
   * #runFor(int)} !</b> <br>
   * <b>WARNING: This is not thread safe! It should either be run from the operator thread or from
   * another thread while the operator is stopped. The results are visible to all threads.</b>
   */
  @Override
  public final void updateMetrics() {
    updateRateAndAlpha();
    updateCostAndSelectivity();
  }

  private void updateCostAndSelectivity() {
    if (tuplesRead.longValue() == 0 || processingTimeNanos.longValue() == 0) {
      return;
    }
    double effectiveProcessingTimeNanos = processingTimeNanos.longValue() / (double) replicaSet.size();
    final double currentSelectivity = tuplesWritten.longValue() / (double) tuplesRead.longValue();
    final double currentCost = effectiveProcessingTimeNanos / (double) tuplesRead.longValue();
    this.selectivity = movingAverage(currentSelectivity, selectivity);
    this.cost = movingAverage(currentCost, cost);
    this.tuplesRead.reset();
    this.tuplesWritten.reset();
    this.processingTimeNanos.reset();
  }

  @Override
  public synchronized void updateMetricsForReplica(int replicaIndex, long tuplesRead, long tuplesWritten, long processingTimeNanos) {
    if(replicaSet.contains(replicaIndex)) {
      updateMetrics();
      replicaSet.clear();
    }
    replicaSet.add(replicaIndex);
    this.tuplesRead.add(tuplesRead);
    this.tuplesWritten.add(tuplesWritten);
    this.processingTimeNanos.add(processingTimeNanos);
  }

  private void updateRateAndAlpha() {
    final long currentTime = System.currentTimeMillis();
    final long updatePeriod = currentTime - lastUpdateTime;
    if (updatePeriod == 0) {
      return;
    }
    // Update alpha value
    this.alpha = Math.min(TARGET_ALPHA, TARGET_ALPHA * updatePeriod / TARGET_UPDATE_PERIOD);
    final double currentRate = tuplesRead.doubleValue() / (double) (MILLIS_TO_NANOS * updatePeriod);
    this.rate = movingAverage(currentRate, rate);
    this.lastUpdateTime = currentTime;
  }

  private double movingAverage(double newValue, double oldValue) {
    return (alpha * newValue) + ((1 - alpha) * oldValue);
  }

  @Override
  public final double getSelectivity() {
    return selectivity;
  }

  @Override
  public final double getCost() {
    return cost;
  }

  @Override
  public final double getRate() {
    return rate;
  }

  public ComponentType getType() {
    return state.getType();
  }

  public final ConnectionsNumber inputsNumber() {
    return state.inputsNumber();
  }

  public final ConnectionsNumber outputsNumber() {
    return state.outputsNumber();
  }

  public void enable() {
    executionTimeMetric.enable();
    rateMetric.enable();
    state.enable();
  }

  public void disable() {
    state.disable();
    executionTimeMetric.disable();
    rateMetric.disable();
  }

  public boolean isEnabled() {
    return state.isEnabled();
  }

  public String getId() {
    return state.getId();
  }


  public int getIndex() {
    return state.getIndex();
  }

  protected <T> boolean isStreamFinished(T tuple, Stream<T> stream) {
    return (tuple == null) && (stream.isFlushed());
  }

  protected void flush() {
    flushAction();
    LOG.info("{} finished processing", getId());
    this.flushed = true;
  }

  protected abstract void flushAction();


  protected boolean isFlushed() {
    return flushed;
  }

  @Override
  public String toString() {
    return getId();
  }

  @Override
  public long getTuplesRead() {
    return tuplesRead.longValue();
  }

  @Override
  public long getTuplesWritten() {
    return tuplesWritten.longValue();
  }

  @Override
  public long getProcessingTimeNanos() {
    return processingTimeNanos.longValue();
  }
}
